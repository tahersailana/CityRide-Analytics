from configs.dags_config import DATABASE_TO_RUN
from airflow.hooks.base import BaseHook
import psycopg2
import snowflake.connector

def run_query(query, params=None):
    if DATABASE_TO_RUN == "POSTGRES":
        conn = None
        try:
            pg_conn = BaseHook.get_connection("postgres_conn")
            conn = psycopg2.connect(
                dbname=pg_conn.schema,
                user=pg_conn.login,
                password=pg_conn.password,
                host=pg_conn.host,
                port=pg_conn.port,
            )
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                if cur.description:
                    result = cur.fetchall()
                else:
                    result = None
                conn.commit()
                return result
        except Exception as e:
            print(f"[ERROR] run_query POSTGRES: {e}")
            raise
        finally:
            if conn:
                conn.close()
    elif DATABASE_TO_RUN == "SNOWFLAKE":
        conn = None
        try:
            sf_conn = BaseHook.get_connection("snowflake_conn")
            extras = sf_conn.extra_dejson
            print(f"[DEBUG] Airflow Snowflake extras: {extras}")
            print(f"[DEBUG] Airflow Snowflake Connection: {sf_conn.__dict__}")
            print(f'=========user={sf_conn.login} password={sf_conn.password}  account={sf_conn.host}, warehouse={sf_conn.extra_dejson.get("warehouse")}, database={sf_conn.schema}, role={sf_conn.extra_dejson.get("role")}, schema={sf_conn.schema}')
            conn = snowflake.connector.connect(
                user=sf_conn.login,
                password=sf_conn.password,
                account=extras.get("account"),
                warehouse=extras.get("warehouse"),
                database=extras.get("database"),
                schema=extras.get("schema"),
                role=extras.get("role"),
            )
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                if cur.description:
                    result = cur.fetchall()
                else:
                    result = None
                conn.commit()
                return result
        except Exception as e:
            print(f"[ERROR] run_query SNOWFLAKE: {e}")
            raise
        finally:
            if conn:
                conn.close()
    else:
        raise ValueError(f"Unsupported DATABASE_TO_RUN value: {DATABASE_TO_RUN}")
