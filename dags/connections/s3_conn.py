# connections/s3_conn.py
import boto3
import logging
from tqdm import tqdm
from boto3.s3.transfer import TransferConfig
from configs.s3_config import S3Config

class S3Connection:
    def __init__(self):
        self.config = S3Config().as_dict()
        logging.info(f"AWS Access Key ID ends with: {self.config['aws_access_key_id'][-4:]}")
        logging.info(f"AWS Secret Access Key ends with: {self.config['aws_secret_access_key'][-4:]}")
        self.session = boto3.session.Session(
            aws_access_key_id=self.config["aws_access_key_id"],
            aws_secret_access_key=self.config["aws_secret_access_key"],
            region_name=self.config["region_name"]
        )
        endpoint = self.config.get("endpoint_url")
        if not endpoint:
            # Connect to real AWS
            self.client = self.session.client("s3")
        else:
            # Connect to custom endpoint (e.g., LocalStack)
            self.client = self.session.client("s3", endpoint_url=endpoint)
        self.bucket_name = self.config["bucket_name"]

    def upload_fileobj(self, file_obj, s3_key):
        """
        Uploads a file-like object to S3.
        """
        self.client.upload_fileobj(file_obj, self.bucket_name, s3_key)

    def list_objects(self, prefix=""):
        """
        Lists object keys in the bucket with the given prefix.
        """
        response = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )
        return [obj["Key"] for obj in response.get("Contents", [])]

    def upload_file_with_progress_bar(self, file_obj, s3_key, filename):
        """
        Uploads a file-like object to S3 with a progress bar.
        """
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

        self.client.upload_fileobj(file_obj, self.bucket_name, s3_key, Config=config, Callback=progress)


# Example usage:
# s3_conn = S3Connection()
# s3_conn.upload_fileobj(file_obj, "folder/file.txt")
# keys = s3_conn.list_objects(prefix="folder/")