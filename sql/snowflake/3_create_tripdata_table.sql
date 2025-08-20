CREATE SCHEMA IF NOT EXISTS CITYRIDE_ANALYTICS;
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
