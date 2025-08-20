-- =====================================================
-- FACT TABLES
-- =====================================================

-- Main Fact Table - Trip Transactions
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.fact_trips (
    trip_id BIGINT PRIMARY KEY,
    
    -- Foreign Keys to Dimensions
    pickup_date_key INT NOT NULL,
    dropoff_date_key INT,
    pickup_time_key INT NOT NULL,
    dropoff_time_key INT,
    pickup_location_id INT,
    dropoff_location_id INT,
    vendor_id INT,
    payment_type_id INT,
    ratecode_id INT,
    trip_type_id INT,
    file_type VARCHAR(20) NOT NULL,
    store_fwd_flag CHAR(1),
    
    -- Measures (Facts)
    passenger_count INT,
    trip_distance DECIMAL(8,2),
    trip_duration_minutes INT,                   -- Calculated field
    
    -- Financial Measures
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    ehail_fee DECIMAL(10,2),
    
    -- FHV Specific Fields
    dispatching_base_num VARCHAR(10),
    affiliated_base_number VARCHAR(50),
    sr_flag CHAR(1),
    
    -- Metadata
    source_year INT NOT NULL,
    source_month INT NOT NULL,
    processed_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints
    FOREIGN KEY (pickup_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (dropoff_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (pickup_time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (dropoff_time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (pickup_location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (dropoff_location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (vendor_id) REFERENCES dim_vendor(vendor_id),
    FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type(payment_type_id),
    FOREIGN KEY (ratecode_id) REFERENCES dim_rate_code(ratecode_id),
    FOREIGN KEY (trip_type_id) REFERENCES dim_trip_type(trip_type_id),
    FOREIGN KEY (file_type) REFERENCES dim_file_source(file_type),
    FOREIGN KEY (store_fwd_flag) REFERENCES dim_store_fwd_flag(store_fwd_flag)
);

-- =====================================================
-- AGGREGATE FACT TABLES (Optional - for performance)
-- =====================================================

-- Daily Aggregated Facts by Location and Service Type
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.fact_trips_daily_agg (
    date_key INT NOT NULL,
    pickup_location_id INT,
    dropoff_location_id INT,
    file_type VARCHAR(20) NOT NULL,
    vendor_id INT,
    
    -- Aggregated Measures
    total_trips INT NOT NULL,
    total_passengers INT,
    total_distance DECIMAL(12,2),
    total_revenue DECIMAL(15,2),
    total_tips DECIMAL(12,2),
    avg_trip_distance DECIMAL(8,2),
    avg_fare_amount DECIMAL(10,2),
    avg_tip_percentage DECIMAL(12,2),
    avg_trip_duration_minutes DECIMAL(8,2),
    
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (date_key, pickup_location_id, dropoff_location_id, file_type, vendor_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (pickup_location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (dropoff_location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (file_type) REFERENCES dim_file_source(file_type),
    FOREIGN KEY (vendor_id) REFERENCES dim_vendor(vendor_id)
);

-- Hourly Aggregated Facts for Time-based Analysis
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.fact_trips_hourly_agg (
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    pickup_location_id INT,
    
    -- Aggregated Measures
    total_trips INT NOT NULL,
    total_passengers INT,
    total_distance DECIMAL(12,2),
    total_revenue DECIMAL(15,2),
    avg_trip_distance DECIMAL(8,2),
    avg_fare_amount DECIMAL(10,2),
    
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (date_key, time_key, file_type, pickup_location_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (file_type) REFERENCES dim_file_source(file_type),
    FOREIGN KEY (pickup_location_id) REFERENCES dim_location(location_id)
);