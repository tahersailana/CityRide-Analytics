# s3_config.py
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

class S3Config:
    def __init__(self):
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL", "")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "test")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        self.region_name = os.getenv("AWS_REGION", "us-east-1")
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "cityride-raw")

    def as_dict(self):
        """Return configuration as a dictionary (useful for boto3)"""
        return {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name,
            "bucket_name": self.bucket_name,
        }
