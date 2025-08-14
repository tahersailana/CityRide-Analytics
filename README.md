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

### 6. Add DAGs
- Place your DAG files in the `./dags` folder.
- Airflow will automatically detect and parse the DAGs.

---

### 7. Testing and Running
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
