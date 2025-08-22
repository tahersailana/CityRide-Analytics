import logging
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
load_dotenv()


def load_data_from_source(query):
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {str(e)}")
        return None