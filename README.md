# CityRide-Analytics

## Project Overview
CityRide-Analytics is an end-to-end data engineering project that ingests, processes, and visualizes NYC Taxi & For-Hire Vehicle trip data. The pipeline uses **Airflow** for orchestration, **PostgreSQL** as a metadata and analytics database, and **LocalStack** to emulate AWS S3 locally. Streamlit dashboards are used to visualize insights.

---

## Prerequisites
- Docker & Docker Compose installed
- Git
- Python 3.8+ (for any local scripts)

---

## Setup Instructions

### 0. Setup Python Virtual Environment
Create a Python 3.11 virtual environment:

```bash
python3.11 -m venv venv
```

Activate the virtual environment:

- On macOS/Linux:

```bash
source venv/bin/activate
```

- On Windows:

```bash
.\venv\Scripts\activate
```

Install Apache Airflow with constraints (replace `AIRFLOW_VERSION` and `PYTHON_VERSION` with the appropriate versions):

```bash
AIRFLOW_VERSION=2.7.1
PYTHON_VERSION=3.11
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

---

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd CityRide-Analytics/CityRide-Analytics
```

### 2. Start Supporting Services
Start PostgreSQL and LocalStack first:

```bash
docker-compose up -d testdb_postgres localstack
```

- Wait until Postgres is ready (`database system is ready to accept connections`) in logs.

---

### 3. Initialize Airflow Database
Use a temporary Airflow container to initialize the metadata database:

```bash
docker-compose run --rm airflow-webserver airflow db init
```

---

### 4. Create Airflow Admin User
```bash
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

---

### 5. Start Airflow Services
```bash
docker-compose up -d airflow-webserver airflow-scheduler
```

- Check logs to ensure services are running:

```bash
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
```

- Airflow Web UI will be available at: [http://localhost:8080](http://localhost:8080)

---

### 6. Setup DAGs and Environment

1. Create a Snowflake account.

2. Run the Snowflake user creation script:
    sql/snowflake_user_creation.sql

3. Create all required tables by executing the SQL files available in the `sql` folder against your Postgres instance or in snowflake.

4. Create the following connections in the Airflow UI:

   - **S3 Connection**:
     - Conn Id: `s3_conn`
     - Conn Type: `S3`
     - Extra: `{"aws_access_key_id": "<your_access_key>", "aws_secret_access_key": "<your_secret_key>", "region_name": "us-east-1", "endpoint_url": "http://localstack:4566"}` (if using LocalStack)

   - **Postgres Connection**:
     - Conn Id: `postgres_conn`
     - Conn Type: `Postgres`
     - Host: `testdb_postgres` (if your postgres is hosted on Docker else localhost)
     - Schema: `test_data`
     - Login: `user`
     - Password: `password123`
     - Port: `5432`

   - **Snowflake Connection**:
     - Conn Id: `snowflake_default`
     - Conn Type: `Snowflake`
     - Login: `<your_snowflake_username>`
     - Password: `<your_snowflake_password>`
     - Account: `<your_snowflake_account>`
     - Warehouse: `<your_snowflake_warehouse>`
     - Database: `<your_snowflake_database>`
     - Schema: `<your_snowflake_schema>`

- Place DAG files in the `./dags` folder.
- Airflow will automatically detect and parse the DAGs.

---

### 8. Testing and Running
- Trigger DAGs manually from the Airflow UI or CLI:

```bash
docker-compose exec airflow-webserver airflow dags trigger <dag_id>
```

- Monitor task execution and logs in the UI.

---

## Notes
- **LocalStack** emulates AWS S3; make sure `DATA_DIR` in `docker-compose.yaml` points to a project-local folder to avoid resource busy errors.
- **Airflow Executor**: The project uses `LocalExecutor` for simplicity; Celery workers are not required.
- **Backfill Support**: Airflow DAGs can be backfilled manually for past months using:

```bash
docker-compose exec airflow-webserver airflow dags backfill <dag_id> -s <start_date> -e <end_date>
```

- Ignore deprecation warnings regarding `sql_alchemy_conn`; they do not prevent functionality.
