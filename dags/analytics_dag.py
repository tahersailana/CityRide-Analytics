import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from utils.helpers import run_query

DATABASE_TO_RUN = Variable.get("database_to_run")

def load_incremental_fact_table(year, month, fact_table):
    if fact_table == 'fact_trips':
        insert_record_query = """ 
        INSERT INTO CITYRIDE_METADATA.etl_analytics_log (
            dag_name, year, month, fact_table_name, status, started_at
        ) VALUES (
            %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        );
        """
        run_query(insert_record_query, ('nyc_trip_analytics_etl', year, month, fact_table, 'started'))

        # Step 1: Load new data into fact_trips
        print("[INFO] Loading data into fact_trips...")
        try:
            fact_trips_query = f"""
            INSERT INTO CITYRIDE_ANALYTICS.fact_trips (
                trip_id,
                pickup_date_key,
                dropoff_date_key,
                pickup_time_key,
                dropoff_time_key,
                pickup_location_id,
                dropoff_location_id,
                vendor_id,
                payment_type_id,
                ratecode_id,
                trip_type_id,
                file_type,
                store_fwd_flag,
                passenger_count,
                trip_distance,
                trip_duration_minutes,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                total_amount,
                congestion_surcharge,
                airport_fee,
                ehail_fee,
                dispatching_base_num,
                affiliated_base_number,
                sr_flag,
                source_year,
                source_month,
                processed_at
            )
            SELECT 
                t.trip_id,
                
                -- Date Keys (convert timestamps to YYYYMMDD format)
                TO_NUMBER(TO_CHAR(t.pickup_datetime, 'YYYYMMDD')) AS pickup_date_key,
                TO_NUMBER(TO_CHAR(t.dropoff_datetime, 'YYYYMMDD')) AS dropoff_date_key,
                
                -- Time Keys (convert timestamps to HHMM format)
                TO_NUMBER(TO_CHAR(t.pickup_datetime, 'HH24MI')) AS pickup_time_key,
                TO_NUMBER(TO_CHAR(t.dropoff_datetime, 'HH24MI')) AS dropoff_time_key,
                
                -- Location Foreign Keys (handle NULLs)
                COALESCE(t.pu_location_id, 0) AS pickup_location_id,
                COALESCE(t.do_location_id, 0) AS dropoff_location_id,
                
                -- Other Foreign Keys (handle NULLs with defaults)
                COALESCE(t.vendor_id, 0) AS vendor_id,
                COALESCE(t.payment_type, 0) AS payment_type_id,
                COALESCE(t.ratecode_id, 1) AS ratecode_id,
                COALESCE(t.trip_type, 0) AS trip_type_id,
                
                -- Direct mappings
                t.file_type,
                COALESCE(t.store_and_fwd_flag, ' ') AS store_fwd_flag,
                
                -- Measures
                t.passenger_count,
                t.trip_distance,
                
                -- Calculate trip duration in minutes
                CASE 
                    WHEN t.pickup_datetime IS NOT NULL AND t.dropoff_datetime IS NOT NULL 
                    THEN DATEDIFF('minute', t.pickup_datetime, t.dropoff_datetime)
                    ELSE NULL 
                END AS trip_duration_minutes,
                
                -- Financial measures
                t.fare_amount,
                t.extra,
                t.mta_tax,
                t.tip_amount,
                t.tolls_amount,
                t.improvement_surcharge,
                t.total_amount,
                t.congestion_surcharge,
                t.airport_fee,
                t.ehail_fee,
                
                -- FHV specific fields
                t.dispatching_base_num,
                t.affiliated_base_number,
                t.sr_flag,
                
                -- Metadata
                t.year AS source_year,
                t.month AS source_month,
                t.processed_at
                
            FROM CITYRIDE_ANALYTICS.trip_data t
            WHERE t.year = {year} 
            AND t.month = {month}
            AND t.pickup_datetime IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM CITYRIDE_ANALYTICS.fact_trips f 
                WHERE f.trip_id = t.trip_id
            );
            """
            result = run_query(fact_trips_query)
            # Better error handling for ROW_COUNT()
            try:
                fact_trips_count = result[0][0]
            except Exception:
                fact_trips_count = 0
                
            print(f"[INFO] Inserted {fact_trips_count} records into fact_trips")

            # Update the most recent log entry for this combination (regardless of status)
            update_record_query = """
            UPDATE CITYRIDE_METADATA.etl_analytics_log
            SET status = %s,
                record_count = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE dag_name = %s
            AND year = %s
            AND month = %s
            AND fact_table_name = %s
            AND started_at = (
                SELECT MAX(started_at) 
                FROM CITYRIDE_METADATA.etl_analytics_log 
                WHERE dag_name = %s 
                AND year = %s 
                AND month = %s 
                AND fact_table_name = %s
            );
            """
            run_query(update_record_query, ('loaded', fact_trips_count, 'nyc_trip_analytics_etl', year, month, fact_table, 'nyc_trip_analytics_etl', year, month, fact_table))

        except Exception as e:
            # Update the most recent log entry for this combination (regardless of status)
            update_record_query = """
            UPDATE CITYRIDE_METADATA.etl_analytics_log
            SET status = %s,
                error_message = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE dag_name = %s
            AND year = %s
            AND month = %s
            AND fact_table_name = %s
            AND started_at = (
                SELECT MAX(started_at) 
                FROM CITYRIDE_METADATA.etl_analytics_log 
                WHERE dag_name = %s 
                AND year = %s 
                AND month = %s 
                AND fact_table_name = %s
            );
            """
            run_query(update_record_query, ('failed', str(e), 'nyc_trip_analytics_etl', year, month, fact_table, 'nyc_trip_analytics_etl', year, month, fact_table))
            raise

    elif fact_table == 'fact_trips_daily_agg':
        # Step 2: Load daily aggregates for the new month
        insert_record_query = """ 
        INSERT INTO CITYRIDE_METADATA.etl_analytics_log (
            dag_name, year, month, fact_table_name, status, started_at
        ) VALUES (
            %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        );
        """
        run_query(insert_record_query, ('nyc_trip_analytics_etl', year, month, fact_table, 'started'))

        print("[INFO] Loading data into fact_trips_daily_agg...")
        try:
            daily_agg_query = f"""
            INSERT INTO CITYRIDE_ANALYTICS.fact_trips_daily_agg (
                date_key,
                pickup_location_id,
                dropoff_location_id,
                file_type,
                vendor_id,
                total_trips,
                total_passengers,
                total_distance,
                total_revenue,
                total_tips,
                avg_trip_distance,
                avg_fare_amount,
                avg_tip_percentage,
                avg_trip_duration_minutes
            )
            SELECT 
                pickup_date_key AS date_key,
                pickup_location_id,
                dropoff_location_id,
                file_type,
                vendor_id,
                
                -- Aggregated measures
                COUNT(*) AS total_trips,
                SUM(passenger_count) AS total_passengers,
                SUM(trip_distance) AS total_distance,
                SUM(total_amount) AS total_revenue,
                SUM(tip_amount) AS total_tips,
                
                -- Averages
                AVG(trip_distance) AS avg_trip_distance,
                AVG(fare_amount) AS avg_fare_amount,
                
                -- Tip percentage calculation
                CASE 
                    WHEN SUM(fare_amount) > 0 
                    THEN (SUM(tip_amount) / SUM(fare_amount)) * 100 
                    ELSE 0 
                END AS avg_tip_percentage,
                
                AVG(trip_duration_minutes) AS avg_trip_duration_minutes
                
            FROM CITYRIDE_ANALYTICS.fact_trips
            WHERE pickup_date_key BETWEEN {year}{month:02d}01 AND {year}{month:02d}31
            AND pickup_location_id IS NOT NULL
            AND dropoff_location_id IS NOT NULL
            AND vendor_id IS NOT NULL
            GROUP BY 
                pickup_date_key,
                pickup_location_id,
                dropoff_location_id,
                file_type,
                vendor_id
            HAVING COUNT(*) > 0;
            """
            
            result = run_query(daily_agg_query)
            
            # Better error handling for ROW_COUNT()
            try:
                daily_agg_count = result[0][0]
            except Exception:
                daily_agg_count = 0
                
            print(f"[INFO] Inserted {daily_agg_count} records into fact_trips_daily_agg")

            # Update the most recent log entry for this combination (regardless of status)
            update_record_query = """
            UPDATE CITYRIDE_METADATA.etl_analytics_log
            SET status = %s,
                record_count = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE dag_name = %s
            AND year = %s
            AND month = %s
            AND fact_table_name = %s
            AND started_at = (
                SELECT MAX(started_at) 
                FROM CITYRIDE_METADATA.etl_analytics_log 
                WHERE dag_name = %s 
                AND year = %s 
                AND month = %s 
                AND fact_table_name = %s
            );
            """
            run_query(update_record_query, ('loaded', daily_agg_count, 'nyc_trip_analytics_etl', year, month, fact_table, 'nyc_trip_analytics_etl', year, month, fact_table))

        except Exception as e:
            # Update the most recent log entry for this combination (regardless of status)
            update_record_query = """
            UPDATE CITYRIDE_METADATA.etl_analytics_log
            SET status = %s,
                error_message = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE dag_name = %s
            AND year = %s
            AND month = %s
            AND fact_table_name = %s
            AND started_at = (
                SELECT MAX(started_at) 
                FROM CITYRIDE_METADATA.etl_analytics_log 
                WHERE dag_name = %s 
                AND year = %s 
                AND month = %s 
                AND fact_table_name = %s
            );
            """
            run_query(update_record_query, ('failed', str(e), 'nyc_trip_analytics_etl', year, month, fact_table, 'nyc_trip_analytics_etl', year, month, fact_table))
            raise
        
    elif fact_table == 'fact_trips_hourly_agg':
        # Step 3: Load hourly aggregates for the new month
        insert_record_query = """ 
        INSERT INTO CITYRIDE_METADATA.etl_analytics_log (
            dag_name, year, month, fact_table_name, status, started_at
        ) VALUES (
            %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        );
        """
        run_query(insert_record_query, ('nyc_trip_analytics_etl', year, month, fact_table, 'started'))

        print("[INFO] Loading data into fact_trips_hourly_agg...")
        try:
            hourly_agg_query = f"""
            INSERT INTO CITYRIDE_ANALYTICS.fact_trips_hourly_agg (
                date_key,
                time_key,
                file_type,
                pickup_location_id,
                total_trips,
                total_passengers,
                total_distance,
                total_revenue,
                avg_trip_distance,
                avg_fare_amount
            )
            SELECT 
                pickup_date_key AS date_key,
                -- Convert time_key to hourly (remove minutes)
                (FLOOR(pickup_time_key / 100) * 100) AS time_key,
                file_type,
                pickup_location_id,
                
                -- Aggregated measures
                COUNT(*) AS total_trips,
                SUM(passenger_count) AS total_passengers,
                SUM(trip_distance) AS total_distance,
                SUM(total_amount) AS total_revenue,
                AVG(trip_distance) AS avg_trip_distance,
                AVG(fare_amount) AS avg_fare_amount
                
            FROM CITYRIDE_ANALYTICS.fact_trips
            WHERE pickup_date_key BETWEEN {year}{month:02d}01 AND {year}{month:02d}31
            AND pickup_time_key IS NOT NULL
            AND pickup_location_id IS NOT NULL
            GROUP BY 
                pickup_date_key,
                (FLOOR(pickup_time_key / 100) * 100),
                file_type,
                pickup_location_id
            HAVING COUNT(*) > 0;
            """
            
            result = run_query(hourly_agg_query)
            
            # Better error handling for ROW_COUNT()
            try:
                hourly_agg_count = result[0][0]
            except Exception:
                hourly_agg_count = 0
                
            print(f"[INFO] Inserted {hourly_agg_count} records into fact_trips_hourly_agg")

            # Update the most recent log entry for this combination (regardless of status)
            update_record_query = """
            UPDATE CITYRIDE_METADATA.etl_analytics_log
            SET status = %s,
                record_count = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE dag_name = %s
            AND year = %s
            AND month = %s
            AND fact_table_name = %s
            AND started_at = (
                SELECT MAX(started_at) 
                FROM CITYRIDE_METADATA.etl_analytics_log 
                WHERE dag_name = %s 
                AND year = %s 
                AND month = %s 
                AND fact_table_name = %s
            );
            """
            run_query(update_record_query, ('loaded', hourly_agg_count, 'nyc_trip_analytics_etl', year, month, fact_table, 'nyc_trip_analytics_etl', year, month, fact_table))

        except Exception as e:
            # Update the most recent log entry for this combination (regardless of status)
            update_record_query = """
            UPDATE CITYRIDE_METADATA.etl_analytics_log
            SET status = %s,
                error_message = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE dag_name = %s
            AND year = %s
            AND month = %s
            AND fact_table_name = %s
            AND started_at = (
                SELECT MAX(started_at) 
                FROM CITYRIDE_METADATA.etl_analytics_log 
                WHERE dag_name = %s 
                AND year = %s 
                AND month = %s 
                AND fact_table_name = %s
            );
            """
            run_query(update_record_query, ('failed', str(e), 'nyc_trip_analytics_etl', year, month, fact_table, 'nyc_trip_analytics_etl', year, month, fact_table))
            raise


def load_incremental_fact_tables(**kwargs):
    # Get year and month from DAG run configuration
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
    
    print(f"[INFO] Processing incremental data for {year}-{month:02d}")

    fact_tables = ['fact_trips','fact_trips_daily_agg','fact_trips_hourly_agg']

    for fact_table in fact_tables:
        check_sql = """
            SELECT status 
            FROM CITYRIDE_METADATA.etl_analytics_log
            WHERE year = %s
            AND month = %s
            AND fact_table_name = %s
            ORDER BY started_at DESC
            LIMIT 1;
        """
        result = run_query(check_sql, (year, month, fact_table))
        if result and result[0][0] == 'loaded':
            logging.info(f"{fact_table} already loaded for {year}-{month:02d}. Skipping this step.")
            continue
        else:
            try:
                load_incremental_fact_table(year, month, fact_table)
            except Exception as e:
                logging.error(f"Error processing {fact_table} Table: {e}", exc_info=True)
                raise
    
    print(f"[SUCCESS] Analytics DAG completed for {year}-{month:02d}")

# DAG Definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nyc_trip_analytics_etl',
    default_args=default_args,
    description='Load incremental NYC trip data into star schema fact tables',
    schedule_interval=None,  # Manual trigger with year/month parameters
    catchup=False,
    tags=['analytics', 'nyc-trips', 'etl']
)

# Single task to load all fact tables
load_fact_tables_task = PythonOperator(
    task_id='load_incremental_fact_tables',
    python_callable=load_incremental_fact_tables,
    dag=dag,
    provide_context=True
)

load_fact_tables_task