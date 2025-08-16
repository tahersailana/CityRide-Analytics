CREATE TABLE cityride_analytics.processed_dag_metadata (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_type VARCHAR(20) NOT NULL,        -- e.g., 'yellow', 'green', 'fhv'
    year INT NOT NULL,
    month INT NOT NULL,
    status VARCHAR(20) DEFAULT NULL,       -- NULL or 'loaded'
    rows_in_file BIGINT DEFAULT 0,
    rows_loaded BIGINT DEFAULT 0,          -- number of rows inserted in trip_data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE cityride_analytics.processed_dag_metadata
ADD CONSTRAINT uq_file_month_year UNIQUE (file_type, year, month);