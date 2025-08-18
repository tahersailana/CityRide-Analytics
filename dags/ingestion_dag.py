from datetime import datetime, timedelta
from io import BytesIO
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from configs.dags_config import FILE_TYPES
from utils.helpers import run_query

# -------------------
# INITIALIZATION
# -------------------
s3_hook = S3Hook(aws_conn_id="s3_conn")

# -------------------
# CONFIG
# -------------------
BUCKET_NAME = Variable.get("s3_bucket_name")
DATABASE_TO_RUN = Variable.get("database_to_run")
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# -------------------
# HELPERS
# -------------------
def get_target_month(execution_date):
    target = (execution_date.replace(day=1) - timedelta(days=1))
    print(f"[DEBUG] Calculated target date: {target.year}-{target.month:02d} (one year back from current run’s target)")
    return target.year - 1, target.month

def init_metadata(**context):
    execution_date = context["execution_date"]
    prev_year, prev_month = get_target_month(execution_date)
    print(f"[DEBUG] Initializing metadata for {prev_year}-{prev_month:02d}")

    postgres_query = f"""
        INSERT INTO cityride_metadata.file_metadata (year, month, file_type, status)
        VALUES (%s, %s, %s, 'MISSING')
        ON CONFLICT (year, month, file_type) DO NOTHING;
        """
    snowflake_query = f"""
        MERGE INTO cityride_metadata.file_metadata t
        USING (SELECT %s AS year, %s AS month, %s AS file_type) s
        ON t.year = s.year AND t.month = s.month AND t.file_type = s.file_type
        WHEN NOT MATCHED THEN
        INSERT (year, month, file_type, status)
        VALUES (s.year, s.month, s.file_type, 'MISSING');
        """
    for ftype in FILE_TYPES.keys():
        if DATABASE_TO_RUN == "POSTGRES":
            run_query(postgres_query, (prev_year, prev_month, ftype))
        elif DATABASE_TO_RUN == "SNOWFLAKE":
            run_query(snowflake_query, (prev_year, prev_month, ftype))
        else:
            raise ValueError(f"Unsupported DATABASE_TO_RUN value: {DATABASE_TO_RUN}")

def process_files(**context):
    execution_date = context["execution_date"] 
    year, month = get_target_month(execution_date)
    print(f"[DEBUG] Starting file processing for {year}-{month:02d}")

    # Check current file statuses
    sql_status = f"""
    SELECT file_type, status FROM cityride_metadata.file_metadata
    WHERE year={year} AND month={month};
    """
    records = run_query(sql_status)
    status_map = {row[0]: row[1] for row in records}

    # If all files are uploaded, mark DAG as success and exit
    if all(status_map.get(ftype) == 'UPLOADED' for ftype in FILE_TYPES):
        print("[INFO] All files already uploaded. Exiting DAG successfully.")
        return

    # Check missing files via API and update status to AVAILABLE if found
    for ftype, fname_tpl in FILE_TYPES.items():
        current_status = status_map.get(ftype)
        if current_status == 'MISSING':
            fname = fname_tpl.format(year=year, month=month)
            url = f"{TLC_BASE_URL}/{fname}"
            try:
                resp = requests.head(url)
                if resp.status_code == 200:
                    print(f"[INFO] File {fname} found via API. Marking as AVAILABLE.")
                    if DATABASE_TO_RUN == "POSTGRES":
                        sql_update = f"""
                        UPDATE cityride_metadata.file_metadata
                        SET status='AVAILABLE', last_checked=now(), updated_at=now()
                        WHERE year={year} AND month={month} AND file_type='{ftype}';
                        """
                        run_query(sql_update)
                    elif DATABASE_TO_RUN == "SNOWFLAKE":
                        sql_update = f"""
                        MERGE INTO cityride_metadata.file_metadata t
                        USING (SELECT {year} AS year, {month} AS month, '{ftype}' AS file_type) s
                        ON t.year = s.year AND t.month = s.month AND t.file_type = s.file_type
                        WHEN MATCHED THEN UPDATE SET status='AVAILABLE', last_checked=CURRENT_TIMESTAMP(), updated_at=CURRENT_TIMESTAMP();
                        """
                        run_query(sql_update)
                    else:
                        raise ValueError(f"Unsupported DATABASE_TO_RUN value: {DATABASE_TO_RUN}")
                    status_map[ftype] = 'AVAILABLE'
                else:
                    print(f"[DEBUG] File {fname} still missing.")
            except Exception as e:
                print(f"[ERROR] Error checking file {fname}: {e}")

    # For AVAILABLE files, download stream and upload to S3, then mark as UPLOADED
    for ftype, status in status_map.items():
        if status == 'AVAILABLE':
            fname = FILE_TYPES[ftype].format(year=year, month=month)
            url = f"{TLC_BASE_URL}/{fname}"
            try:
                resp = requests.get(url, stream=True)
                if resp.status_code == 200:
                    print(f"[INFO] Downloading and uploading {fname} to S3.")
                    file_obj = BytesIO()
                    for chunk in resp.iter_content(chunk_size=8192):
                        file_obj.write(chunk)
                    file_obj.seek(0)
                    s3_key = f"raw/{fname}"
                    s3_hook.load_file_obj(file_obj, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                    print(f"[INFO] Uploaded {fname} to S3.")

                    # Update status to UPLOADED
                    if DATABASE_TO_RUN == "POSTGRES":
                        sql_update = f"""
                        UPDATE cityride_metadata.file_metadata
                        SET status='UPLOADED', uploaded_at=now(), updated_at=now()
                        WHERE year={year} AND month={month} AND file_type='{ftype}';
                        """
                        run_query(sql_update)
                    elif DATABASE_TO_RUN == "SNOWFLAKE":
                        sql_update = f"""
                        MERGE INTO cityride_metadata.file_metadata t
                        USING (SELECT {year} AS year, {month} AS month, '{ftype}' AS file_type) s
                        ON t.year = s.year AND t.month = s.month AND t.file_type = s.file_type
                        WHEN MATCHED THEN UPDATE SET status='UPLOADED', uploaded_at=CURRENT_TIMESTAMP(), updated_at=CURRENT_TIMESTAMP();
                        """
                        run_query(sql_update)
                    else:
                        raise ValueError(f"Unsupported DATABASE_TO_RUN value: {DATABASE_TO_RUN}")
                    status_map[ftype] = 'UPLOADED'
                else:
                    print(f"[WARN] Unable to download file {fname}. HTTP status {resp.status_code}")
            except Exception as e:
                print(f"[ERROR] Failed to process {fname}: {e}")

    # Final verification: check if all files are uploaded
    if DATABASE_TO_RUN == "POSTGRES":
        sql_final = f"""
        SELECT COUNT(*) FROM cityride_metadata.file_metadata
        WHERE year={year} AND month={month} AND status='UPLOADED';
        """
    elif DATABASE_TO_RUN == "SNOWFLAKE":
        sql_final = f"""
        SELECT COUNT(*) FROM cityride_metadata.file_metadata
        WHERE year={year} AND month={month} AND status='UPLOADED';
        """
    else:
        raise ValueError(f"Unsupported DATABASE_TO_RUN value: {DATABASE_TO_RUN}")

    result = run_query(sql_final)
    uploaded_count = result[0][0] if result else 0
    if uploaded_count == len(FILE_TYPES):
        print(f"[INFO] All {uploaded_count} files uploaded successfully for {year}-{month:02d}.")
    else:
        print(f"[WARN] Only {uploaded_count} out of {len(FILE_TYPES)} files uploaded for {year}-{month:02d}. DAG will retry.")
        raise RuntimeError(f"Only {uploaded_count} out of {len(FILE_TYPES)} files uploaded for {year}-{month:02d}. Marking DAG as failed.")

# -------------------
# DAG DEFINITION
# -------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(days=2),
}

dag = DAG(
    dag_id="nyc_taxi_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 2 * *",
    catchup=False,
    tags=["cityride", "ingestion"],
)

# -------------------
# TASKS
# -------------------

t_init_metadata = PythonOperator(
    task_id="init_metadata",
    python_callable=init_metadata,
    provide_context=True,
    dag=dag,
)

t_process_files = PythonOperator(
    task_id="process_files",
    python_callable=process_files,
    provide_context=True,
    dag=dag,
)

# t_trigger_processing_dag = TriggerDagRunOperator(
#     task_id='trigger_processing_dag',
#     trigger_dag_id='nyc_taxi_processing',
#     conf={
#         "year": "{{ execution_date.year - 1 }}",  # matches get_target_month logic
#         "month": "{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).month }}"
#     },
#     wait_for_completion=False,  # or True if you want to wait
#     dag=dag,
# )

t_init_metadata >> t_process_files 