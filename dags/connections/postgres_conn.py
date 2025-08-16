# connections/postgres_conn.py
import psycopg2
from configs.db_config import PostgresConfig

POSTGRES_CONFIG = PostgresConfig().as_dict()

def run_query(sql):
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"],
    )
    cur = conn.cursor()
    cur.execute(sql)
    
    try:
        result = cur.fetchall()  # only works for SELECT
    except psycopg2.ProgrammingError:
        result = None  # for INSERT/UPDATE/DELETE
    
    conn.commit()
    cur.close()
    conn.close()
    return result

# Bulk insert function
def insert_records(sql, records):
    """
    Bulk insert records into the database using the provided SQL statement.
    Args:
        sql (str): SQL INSERT statement with placeholders.
        records (list of tuple): List of tuples representing rows to insert.
    """
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"],
    )
    cur = conn.cursor()
    cur.executemany(sql, records)
    conn.commit()
    cur.close()
    conn.close()
