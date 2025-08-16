from datetime import datetime, timedelta
import logging
import requests
from io import BytesIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from connections.postgres_conn import run_query
from connections.s3_conn import S3Connection
# from connections.s3_conn import upload_file_with_progress_bar, s3_client
from configs.dags_config import FILE_TYPES
from configs.s3_config import S3Config #to-do remove
config = S3Config().as_dict() #to-do remove
# -------------------
# INITALIZATION
# -------------------
s3_conn = S3Connection()

# -------------------
# CONFIG
# -------------------
BUCKET_NAME = s3_conn.bucket_name
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
    for ftype in FILE_TYPES.keys():
        sql = f"""
        INSERT INTO cityride_analytics.file_metadata (year, month, file_type, status)
        VALUES ({prev_year}, {prev_month}, '{ftype}', 'MISSING')
        ON CONFLICT (year, month, file_type) DO NOTHING;
        """
        run_query(sql)

def process_files(**context):
    logging.info(f"AWS Access Key ID ends with: {config['aws_access_key_id'][-4:]}") #to-do remove
    logging.info(f"AWS Secret Access Key ends with: {config['aws_secret_access_key'][-4:]}") #to-do remove
    execution_date = context["execution_date"] 
    year, month = get_target_month(execution_date)
    print(f"[DEBUG] Starting file processing for {year}-{month:02d}")

    # Check current file statuses
    sql_status = f"""
    SELECT file_type, status FROM cityride_analytics.file_metadata
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
                    sql_update = f"""
                    UPDATE cityride_analytics.file_metadata
                    SET status='AVAILABLE', last_checked=now(), updated_at=now()
                    WHERE year={year} AND month={month} AND file_type='{ftype}';
                    """
                    run_query(sql_update)
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
                    # s3_client.upload_fileobj(file_obj, BUCKET_NAME, s3_key)
                    s3_conn.upload_file_with_progress_bar(file_obj,s3_key,fname)
                    print(f"[INFO] Uploaded {fname} to S3.")

                    # Update status to UPLOADED
                    sql_update = f"""
                    UPDATE cityride_analytics.file_metadata
                    SET status='UPLOADED', uploaded_at=now(), updated_at=now()
                    WHERE year={year} AND month={month} AND file_type='{ftype}';
                    """
                    run_query(sql_update)
                    status_map[ftype] = 'UPLOADED'
                else:
                    print(f"[WARN] Unable to download file {fname}. HTTP status {resp.status_code}")
            except Exception as e:
                print(f"[ERROR] Failed to process {fname}: {e}")

    # Final verification: check if all files are uploaded
    sql_final = f"""
    SELECT COUNT(*) FROM cityride_analytics.file_metadata
    WHERE year={year} AND month={month} AND status='UPLOADED';
    """
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

trigger_processing_dag = TriggerDagRunOperator(
    task_id='trigger_processing_dag',
    trigger_dag_id='nyc_taxi_processing',
    conf={
        "year": "{{ execution_date.year - 1 }}",  # matches get_target_month logic
        "month": "{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).month }}"
    },
    wait_for_completion=False,  # or True if you want to wait
    dag=dag,
)

t_init_metadata >> t_process_files >> trigger_processing_dag