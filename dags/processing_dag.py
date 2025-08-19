from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import logging
import gc

from transformations.common_transforms import (
    parse_timestamps,
    correct_numeric_types,
    map_columns_to_table
)
from utils.helpers import run_query

# -------------------
# INITALIZATION
# -------------------
s3_hook = S3Hook(aws_conn_id="s3_conn")

# -------------------
# CONFIG
# -------------------
BUCKET_NAME = Variable.get("s3_bucket_name")
DATABASE_TO_RUN = Variable.get("database_to_run")
S3_PREFIX = "raw/"

# -------------------
# DAG DEFINITION
# -------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(hours=1),
}

dag = DAG(
    dag_id="nyc_taxi_processing",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=["cityride", "processing"],
)

# -------------------
# HELPERS
# -------------------

def insert_records(sql, records):
    """Helper function to insert records using run_query"""
    if not records:
        return
    
    if DATABASE_TO_RUN == "POSTGRES":
        # For PostgreSQL, execute with records as parameters
        run_query(sql, records)
    else:
        # For Snowflake, execute individual inserts or use executemany
        for record in records:
            run_query(sql, record)

def get_snowflake_copy_command(file_type, year, month):
    """Generate dynamic COPY command for Snowflake based on file type"""
    table_mapping = {
        'yellow': 'yellow_raw',
        'green': 'green_raw',
        'fhv': 'fhv_raw'
    }
    
    raw_table = table_mapping.get(file_type.lower())
    if not raw_table:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    copy_command = f"""
    COPY INTO CITYRIDE_METADATA.{raw_table}
    FROM @CITYRIDE_METADATA.cityride_stage_raw
    FILE_FORMAT = (FORMAT_NAME = CITYRIDE_METADATA.cityride_parquet_format)
    PATTERN = '.*{file_type}_tripdata_{year}-{month:02d}.*.parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
    """
    
    return copy_command

def get_trip_data_insert_query(file_type, year, month):
    """Generate INSERT query for trip_data table based on file type"""
    
    if file_type.lower() == 'yellow':
        return f"""
        INSERT INTO cityride_analytics.trip_data (
            file_type, year, month,
            pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
            payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
            improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
            vendor_id, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id
        )
        SELECT
            'yellow' AS file_type,
            {year} AS year,
            {month} AS month,
            TRY_TO_TIMESTAMP_TZ(tpep_pickup_datetime) AS pickup_datetime,
            TRY_TO_TIMESTAMP_TZ(tpep_dropoff_datetime) AS dropoff_datetime,
            passenger_count::INT AS passenger_count,
            trip_distance::DECIMAL(8,2) AS trip_distance,
            payment_type::INT AS payment_type,
            fare_amount::DECIMAL(10,2) AS fare_amount,
            extra::DECIMAL(10,2) AS extra,
            mta_tax::DECIMAL(10,2) AS mta_tax,
            tip_amount::DECIMAL(10,2) AS tip_amount,
            tolls_amount::DECIMAL(10,2) AS tolls_amount,
            improvement_surcharge::DECIMAL(10,2) AS improvement_surcharge,
            total_amount::DECIMAL(10,2) AS total_amount,
            congestion_surcharge::DECIMAL(10,2) AS congestion_surcharge,
            Airport_fee::DECIMAL(10,2) AS airport_fee,
            VendorID::INT AS vendor_id,
            RatecodeID::INT AS ratecode_id,
            store_and_fwd_flag::CHAR(1) AS store_and_fwd_flag,
            PULocationID::INT AS pu_location_id,
            DOLocationID::INT AS do_location_id
        FROM CITYRIDE_METADATA.yellow_raw
        WHERE TRY_TO_TIMESTAMP_TZ(tpep_pickup_datetime) IS NOT NULL;
        """
    
    elif file_type.lower() == 'green':
        return f"""
        INSERT INTO cityride_analytics.trip_data (
            file_type, year, month,
            pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
            payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
            improvement_surcharge, total_amount, congestion_surcharge, ehail_fee, trip_type,
            vendor_id, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id
        )
        SELECT
            'green' AS file_type,
            {year} AS year,
            {month} AS month,
            TRY_TO_TIMESTAMP_TZ(lpep_pickup_datetime) AS pickup_datetime,
            TRY_TO_TIMESTAMP_TZ(lpep_dropoff_datetime) AS dropoff_datetime,
            passenger_count::INT AS passenger_count,
            trip_distance::DECIMAL(8,2) AS trip_distance,
            TRY_CAST(payment_type AS INT) AS payment_type,
            fare_amount::DECIMAL(10,2) AS fare_amount,
            extra::DECIMAL(10,2) AS extra,
            mta_tax::DECIMAL(10,2) AS mta_tax,
            tip_amount::DECIMAL(10,2) AS tip_amount,
            tolls_amount::DECIMAL(10,2) AS tolls_amount,
            improvement_surcharge::DECIMAL(10,2) AS improvement_surcharge,
            total_amount::DECIMAL(10,2) AS total_amount,
            congestion_surcharge::DECIMAL(10,2) AS congestion_surcharge,
            ehail_fee::DECIMAL(10,2) AS ehail_fee,
            TRY_CAST(trip_type AS INT) AS trip_type,
            TRY_CAST(VendorID AS INT) AS vendor_id,
            TRY_CAST(RatecodeID AS INT) AS ratecode_id,
            store_and_fwd_flag::CHAR(1) AS store_and_fwd_flag,
            TRY_CAST(PULocationID AS INT) AS pu_location_id,
            TRY_CAST(DOLocationID AS INT) AS do_location_id
        FROM CITYRIDE_METADATA.green_raw
        WHERE TRY_TO_TIMESTAMP_TZ(lpep_pickup_datetime) IS NOT NULL;
        """
    
    elif file_type.lower() == 'fhv':
        return f"""
        INSERT INTO cityride_analytics.trip_data (
            file_type, year, month,
            pickup_datetime, dropoff_datetime,
            pu_location_id, do_location_id,
            dispatching_base_num, affiliated_base_number, sr_flag
        )
        SELECT
            'fhv' AS file_type,
            {year} AS year,
            {month} AS month,
            TRY_TO_TIMESTAMP_TZ(pickup_datetime) AS pickup_datetime,
            TRY_TO_TIMESTAMP_TZ(dropoff_datetime) AS dropoff_datetime,
            TRY_CAST(PULocationID AS INT) AS pu_location_id,
            TRY_CAST(DOlocationID AS INT) AS do_location_id,
            dispatching_base_num,
            affiliated_base_number,
            sr_flag::CHAR(1) AS sr_flag
        FROM CITYRIDE_METADATA.fhv_raw
        WHERE TRY_TO_TIMESTAMP_TZ(pickup_datetime) IS NOT NULL;
        """
    
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

def process_snowflake_file(file_type, year, month):
    """Process file using Snowflake COPY commands"""
    logging.info(f"Processing {file_type} file for {year}-{month:02d} using Snowflake")
    
    # Step 1: Truncate raw table
    table_mapping = {
        'yellow': 'yellow_raw',
        'green': 'green_raw',
        'fhv': 'fhv_raw'
    }
    
    raw_table = table_mapping.get(file_type.lower())
    truncate_sql = f"TRUNCATE TABLE CITYRIDE_METADATA.{raw_table};"
    
    try:
        logging.info(f"Truncating table {raw_table}")
        run_query(truncate_sql)
        
        # Step 2: Execute COPY command
        copy_sql = get_snowflake_copy_command(file_type, year, month)
        logging.info(f"Executing COPY command: {copy_sql}")
        run_query(copy_sql)

        # Step 2a: Get row count from raw table
        count_sql = f"SELECT COUNT(*) FROM CITYRIDE_METADATA.{raw_table};"
        result = run_query(count_sql)
        rows_in_file = result[0][0] if result else 0
        
        # Step 3: Insert into trip_data
        insert_sql = get_trip_data_insert_query(file_type, year, month)
        logging.info(f"Inserting data into trip_data table")
        run_query(insert_sql)

        # Step 3a: Get row count from trip_data for this specific file
        trip_count_sql = f"""
        SELECT COUNT(*) FROM cityride_analytics.trip_data 
        WHERE file_type = '{file_type}' AND year = {year} AND month = {month};
        """
        result = run_query(trip_count_sql)
        rows_loaded = result[0][0] if result else 0
        
        # Step 4: Update metadata
        metadata_sql = """
            MERGE INTO CITYRIDE_METADATA.etl_processing_log t
            USING (SELECT %s AS file_type, %s AS year, %s AS month, %s AS status, %s AS rows_in_file, %s AS rows_loaded) s
            ON t.file_type = s.file_type AND t.year = s.year AND t.month = s.month
            WHEN MATCHED THEN UPDATE SET
                status = s.status,
                rows_in_file = s.rows_in_file,
                rows_loaded = s.rows_loaded,
                processing_end_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT
                (file_type, year, month, status, rows_in_file, rows_loaded, processing_start_time, processing_end_time, created_at, updated_at)
            VALUES (s.file_type, s.year, s.month, s.status, s.rows_in_file, s.rows_loaded, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        run_query(metadata_sql, (file_type, year, month, 'loaded', rows_in_file, rows_loaded))
        
        logging.info(f"Successfully processed {file_type} file for {year}-{month:02d}")
        
    except Exception as e:
        logging.error(f"Error processing {file_type} file: {e}", exc_info=True)
        # Update metadata with error status
        error_metadata_sql = """
            MERGE INTO CITYRIDE_METADATA.etl_processing_log t
            USING (SELECT %s AS file_type, %s AS year, %s AS month, %s AS status, %s AS error_message) s
            ON t.file_type = s.file_type AND t.year = s.year AND t.month = s.month
            WHEN MATCHED THEN UPDATE SET
                status = s.status,
                error_message = s.error_message,
                processing_end_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT
                (file_type, year, month, status, error_message, processing_start_time, processing_end_time, created_at, updated_at)
            VALUES (s.file_type, s.year, s.month, s.status, s.error_message, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        run_query(error_metadata_sql, (file_type, year, month, 'error', str(e)))
        raise

def process_postgres_file(s3_key, file_type, year, month):
    """Process file using existing PostgreSQL logic"""
    logging.info(f"Processing file: {s3_key} of type {file_type} using PostgreSQL")
    
    # Read file in streaming mode using PyArrow ParquetFile and row groups
    obj = s3_hook.get_key(s3_key, bucket_name=BUCKET_NAME)
    file_bytes = obj.get()['Body'].read()
    pq_file = pq.ParquetFile(pa.BufferReader(file_bytes))
    total_rows = sum(pq_file.metadata.row_group(i).num_rows for i in range(pq_file.num_row_groups))
    logging.info(f'total_rows={total_rows}')
    num_row_groups = pq_file.num_row_groups
    rows_loaded = 0  # cumulative rows inserted

    for rg in range(num_row_groups):
        table = pq_file.read_row_group(rg)
        df = table.to_pandas()

        # Apply transformations
        df = map_columns_to_table(df, file_type)
        logging.info(f"After mapping columns: {len(df)} rows")
        df = parse_timestamps(df, timestamp_cols=['pickup_datetime', 'dropoff_datetime'])
        logging.info(f"After parsing timestamps: {len(df)} rows")
        df = correct_numeric_types(df, numeric_cols=['fare_amount','trip_distance','passenger_count'])
        logging.info(f"After numeric conversion: {len(df)} rows")

        # Add metadata
        df['file_type'] = file_type
        df['year'] = year
        df['month'] = month
        df['processed_at'] = pd.Timestamp.utcnow()

        # Replace pd.NA / np.nan with None for DB
        df = df.where(pd.notnull(df), None)
        df = df.replace(["NaN", "nan", "NAN"], None)

        # Add debug logs to check for column mismatch
        records = [tuple(x) for x in df.to_numpy()]
        logging.info(f"Columns ({len(df.columns)}): {list(df.columns)}")
        if records:
            logging.info(f"First record length: {len(records[0])}")
        else:
            logging.info("No records to insert for this chunk.")

        cols_str = ','.join(df.columns)
        vals_str = ','.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO cityride_analytics.trip_data ({cols_str}) VALUES ({vals_str})"

        # Insert in chunks
        chunk_size = 100000
        try:
            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start+chunk_size].copy()
                records = [tuple(x) for x in chunk.to_numpy()]
                logging.info(f"Inserting chunk {start//chunk_size + 1} with {len(records)} records")
                insert_records(sql, records)
                rows_loaded += len(records)
                # Clear memory
                del chunk, records
                gc.collect()
                # After inserting each row group chunk, update processed_dag_metadata
                metadata_sql = """
                    INSERT INTO cityride_analytics.processed_dag_metadata
                    (file_type, year, month, status, rows_in_file, rows_loaded, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (file_type, year, month)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        rows_loaded = EXCLUDED.rows_loaded,
                        rows_in_file = EXCLUDED.rows_in_file,
                        updated_at = CURRENT_TIMESTAMP;
                """
                insert_records(metadata_sql, [(file_type, year, month, 'loaded', total_rows, rows_loaded)])
        except Exception as e:
            logging.error(f"Error inserting records from file {s3_key} row group {rg}: {e}", exc_info=True)
        finally:
            # Clear memory immediately after processing row group
            del df
            gc.collect()

def process_month_files(**kwargs):
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    year = conf.get("year")
    month = conf.get("month")
    
    # Ensure year and month are integers
    if isinstance(year, str):
        year = int(year)
    if isinstance(month, str):
        month = int(month)
    
    logging.info(f'Processing data for year={year} month={month} using database={DATABASE_TO_RUN}')
    
    if not year or not month:
        raise ValueError("Year and month must be provided in DAG run conf")

    if DATABASE_TO_RUN == "SNOWFLAKE":
        # For Snowflake, process each file type
        file_types = ['yellow', 'green', 'fhv']  # Skip fhvhv as per original logic
        
        for file_type in file_types:
            try:
                process_snowflake_file(file_type, year, month)
            except Exception as e:
                logging.error(f"Error processing {file_type} files: {e}", exc_info=True)
                # Continue processing other file types
                continue
                
    elif DATABASE_TO_RUN == "POSTGRES":
        # For PostgreSQL, use existing logic
        prefix = f"{S3_PREFIX}"
        keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
        files = [key for key in keys if f"{year}-{month:02d}" in key]

        for s3_key in files:
            file_type = s3_key.split('/')[-1].split('_')[0]
            logging.info(f"Processing file: {s3_key} of type {file_type}")

            # Skip FHV and FHVHV files
            if file_type.lower() == 'fhvhv':
                logging.info(f"Skipping file {s3_key} of type {file_type}")
                continue

            try:
                process_postgres_file(s3_key, file_type, year, month)
            except Exception as e:
                logging.error(f"Error processing file {s3_key}: {e}", exc_info=True)
                continue
    else:
        raise ValueError(f"Unsupported DATABASE_TO_RUN value: {DATABASE_TO_RUN}")

# -------------------
# TASKS
# -------------------
t_process_month_files = PythonOperator(
    task_id="process_month_files",
    python_callable=process_month_files,
    dag=dag,
)