from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Create SQLAlchemy engine for Snowflake
engine = create_engine(URL(
    user='AIRFLOW_USER',
    password='StrongPassword123!',
    account='ekorbhk-no98289',
    warehouse='AIRFLOW_WH',
    database='CITYRIDE_ANALYTICS',
    schema='CITYRIDE_ANALYTICS',
    role='AIRFLOW_ROLE'
))
