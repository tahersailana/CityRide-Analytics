# connections/postgres_conn.py
POSTGRES_CONFIG = {
    "host": "testdb_postgres",
    "port": 5432,
    "database": "test_data",
    "user": "user",
    "password": "password123",
}

import psycopg2

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

POSTGRES_CONFIG_lOCAL = {
    "host": "localhost",
    "port": 5434,
    "database": "test_data",
    "user": "user",
    "password": "password123",
}
def insert_records_local(sql, records):
    """
    Bulk insert records into the database using the provided SQL statement.
    Args:
        sql (str): SQL INSERT statement with placeholders.
        records (list of tuple): List of tuples representing rows to insert.
    """
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG_lOCAL["host"],
        port=POSTGRES_CONFIG_lOCAL["port"],
        database=POSTGRES_CONFIG_lOCAL["database"],
        user=POSTGRES_CONFIG_lOCAL["user"],
        password=POSTGRES_CONFIG_lOCAL["password"],
    )
    cur = conn.cursor()
    cur.executemany(sql, records)
    conn.commit()
    cur.close()
    conn.close()