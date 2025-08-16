from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from connections.postgres_conn import insert_records
from connections.s3_conn import S3Connection
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

# -------------------
# INITALIZATION
# -------------------
s3_conn = S3Connection()
s3_client = s3_conn.client

# -------------------
# CONFIG
# -------------------
BUCKET_NAME = s3_conn.bucket_name
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
    schedule_interval=None,  # Triggered externally
    catchup=False,
)

# -------------------
# HELPERS
# -------------------
def process_month_files(**kwargs):
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    year = conf.get("year")
    month = conf.get("month")
    logging.info(f'year={year} month={month} ')
    if not year or not month:
        raise ValueError("Year and month must be provided in DAG run conf")

    prefix = f"{S3_PREFIX}"
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if f"{year}-{month:02d}" in obj['Key']]

    for s3_key in files:
        file_type = s3_key.split('/')[-1].split('_')[0]
        logging.info(f"Processing file: {s3_key} of type {file_type}")

        # Skip FHV and FHVHV files
        if file_type.lower() == 'fhvhv':
            logging.info(f"Skipping file {s3_key} of type {file_type}")
            continue

        # Read file in streaming mode using PyArrow ParquetFile and row groups
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        file_bytes = obj['Body'].read()
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
            # df = filter_invalid_trips(df)
            # logging.info(f"After filtering invalid trips: {len(df)} rows")

            # Add metadata
            df['file_type'] = file_type
            df['year'] = year
            df['month'] = month
            df['processed_at'] = pd.Timestamp.utcnow()

            # Replace pd.NA / np.nan with None for DB
            df = df.where(pd.notnull(df), None)

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

# -------------------
# TASKS
# -------------------
t_process_month_files = PythonOperator(
    task_id="process_month_files",
    python_callable=process_month_files,
    dag=dag,
)
