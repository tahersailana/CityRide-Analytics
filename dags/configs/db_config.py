# db_config.py
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

class PostgresConfig:
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "testdb_postgres")
        self.port = int(os.getenv("POSTGRES_PORT", 5432))
        self.database = os.getenv("POSTGRES_DB", "test_data")
        self.user = os.getenv("POSTGRES_USER", "user")
        self.password = os.getenv("POSTGRES_PASSWORD", "password123")

    def as_dict(self):
        """Return configuration as a dictionary (useful for DB connections)"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }
