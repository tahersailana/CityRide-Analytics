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