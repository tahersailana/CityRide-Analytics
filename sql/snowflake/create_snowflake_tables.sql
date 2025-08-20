-- sign is as airflow user 
CREATE SCHEMA IF NOT EXISTS CITYRIDE_METADATA;
CREATE SCHEMA IF NOT EXISTS CITYRIDE_ANALYTICS;

                                                    -- metadata tables 
CREATE OR REPLACE TABLE CITYRIDE_METADATA.file_metadata (
    id INT AUTOINCREMENT PRIMARY KEY, 
    year INT NOT NULL,
    month INT NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'MISSING',
    last_checked TIMESTAMP_NTZ,        
    uploaded_at TIMESTAMP_NTZ,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP, 
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE CITYRIDE_METADATA.etl_processing_log (
    log_id BIGINT AUTOINCREMENT PRIMARY KEY,
    file_type VARCHAR(20) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'loaded', 'error', 'processing'
    rows_in_file BIGINT,
    rows_loaded BIGINT,
    processing_start_time TIMESTAMP_TZ,
    processing_end_time TIMESTAMP_TZ,
    error_message TEXT,
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (file_type, year, month)
);

CREATE TABLE CITYRIDE_METADATA.etl_analytics_log (
    id BIGINT AUTOINCREMENT PRIMARY KEY,
    dag_name VARCHAR(200) NOT NULL,           
    year INT NOT NULL,
    month INT NOT NULL,
    fact_table_name VARCHAR(200) NOT NULL,    -- e.g., 'fact_trips', 'fact_trips_daily_agg'
    status VARCHAR(50) NOT NULL,              -- 'started', 'loaded', 'failed', 'skipped'
    record_count INT,                         -- rows inserted
    error_message STRING,                     -- if failed
    started_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP_TZ
);

                                                    -- create raw file tables 

-- TABLE yellow_raw
CREATE OR REPLACE TABLE CITYRIDE_METADATA.yellow_raw (
    VendorID STRING,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance FLOAT,
    RatecodeID STRING,
    store_and_fwd_flag STRING,
    PULocationID STRING,
    DOLocationID STRING,
    payment_type STRING,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT
    -- add more if needed
);

-- TABLE green_raw
CREATE OR REPLACE TABLE CITYRIDE_METADATA.green_raw (
    VendorID                   STRING,           -- Changed from vendor_id
    lpep_pickup_datetime       STRING,
    lpep_dropoff_datetime      STRING,
    store_and_fwd_flag         STRING,
    RatecodeID                 STRING,           -- Changed from ratecode_id
    PULocationID               STRING,           -- Changed from pu_location_id
    DOLocationID               STRING,           -- Changed from do_location_id
    passenger_count            INT,
    trip_distance              FLOAT,
    fare_amount                FLOAT,
    extra                      FLOAT,
    mta_tax                    FLOAT,
    tip_amount                 FLOAT,
    tolls_amount               FLOAT,
    ehail_fee                  FLOAT,
    improvement_surcharge      FLOAT,
    total_amount               FLOAT,
    payment_type               STRING,
    trip_type                  STRING,
    congestion_surcharge       FLOAT
);

-- TABLE fhv_raw
CREATE OR REPLACE TABLE CITYRIDE_METADATA.fhv_raw (
    dispatching_base_num       STRING,
    pickup_datetime            STRING,
    dropoff_datetime           STRING,
    DOlocationID               STRING,
    PUlocationID               STRING,
    sr_flag                    STRING,
    affiliated_base_number     STRING
);

                                                -- table  trip_data
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.trip_data (
    trip_id BIGINT AUTOINCREMENT PRIMARY KEY,
    file_type VARCHAR(20) NOT NULL,                          
    year INT NOT NULL,
    month INT NOT NULL,
    processed_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,        

    -- Core trip info
    pickup_datetime TIMESTAMP_TZ,
    dropoff_datetime TIMESTAMP_TZ,
    passenger_count INT,  -- not working in yellow
    trip_distance DECIMAL(8,2),
    
    -- Payment and fare info
    payment_type INT,   -- not working in yellow
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2), -- not working in yellow
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2), -- not working in yellow
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    ehail_fee DECIMAL(10,2),
    trip_type INT,

    -- Vendor / Rate info
    vendor_id INT,
    ratecode_id INT,
    store_and_fwd_flag CHAR(1),

    -- Location info
    pu_location_id INT,
    do_location_id INT,

    -- Basic FHV fields only
    dispatching_base_num VARCHAR(10),
    affiliated_base_number VARCHAR(50),  -- Fixed: missing closing parenthesis
    sr_flag CHAR(1),                     -- Fixed: removed trailing comma
     
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);
