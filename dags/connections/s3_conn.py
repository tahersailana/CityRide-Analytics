# connections/s3_conn.py
import os
import boto3

# Configuration: environment variables can override defaults
S3_CONFIG = {
    "endpoint_url": os.getenv("S3_ENDPOINT_URL", "http://localstack:4566"),  # e.g., "http://localstack:4566" for LocalStack
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "test"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    "region_name": os.getenv("AWS_REGION", "us-east-1"),
    "bucket_name": os.getenv("S3_BUCKET_NAME", "cityride-raw"),
}

# Create a reusable boto3 session
s3_session = boto3.session.Session(
    aws_access_key_id=S3_CONFIG["aws_access_key_id"],
    aws_secret_access_key=S3_CONFIG["aws_secret_access_key"],
    region_name=S3_CONFIG["region_name"]
)

# Create a client for direct operations
s3_client = s3_session.client(
    "s3",
    endpoint_url=S3_CONFIG["endpoint_url"]  # None works for real AWS
)

# Optional helper function for uploading files
def upload_fileobj(file_obj, s3_key):
    """
    Uploads a file-like object to S3.
    """
    s3_client.upload_fileobj(file_obj, S3_CONFIG["bucket_name"], s3_key)

# Optional helper function for listing objects
def list_objects(prefix=""):
    response = s3_client.list_objects_v2(
        Bucket=S3_CONFIG["bucket_name"],
        Prefix=prefix
    )
    return [obj["Key"] for obj in response.get("Contents", [])]

def upload_file_with_progress_bar(file_obj, s3_key, filename):
    from tqdm import tqdm
    from boto3.s3.transfer import TransferConfig

    file_obj.seek(0)
    filesize = len(file_obj.getbuffer())

    class ProgressPercentage:
        def __init__(self, filename, filesize):
            self._filename = filename
            self._filesize = filesize
            self._seen_so_far = 0
            self.pbar = tqdm(total=filesize, unit='B', unit_scale=True, desc=filename)

        def __call__(self, bytes_amount):
            self._seen_so_far += bytes_amount
            self.pbar.update(bytes_amount)
            if self._seen_so_far >= self._filesize:
                self.pbar.close()

    progress = ProgressPercentage(filename, filesize)
    config = TransferConfig(
        multipart_threshold=1024*25,
        max_concurrency=4,
        multipart_chunksize=1024*25,
        use_threads=True
    )

    s3_client.upload_fileobj(file_obj, S3_CONFIG["bucket_name"], s3_key, Config=config, Callback=progress)