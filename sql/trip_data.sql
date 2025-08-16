CREATE TABLE cityride_analytics.trip_data (
    trip_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- keep as BIGINT for identity
    file_type VARCHAR(20) NOT NULL,                          
    year INT NOT NULL,
    month INT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,        

    -- Common pickup/dropoff and trip info
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count FLOAT,
    trip_distance FLOAT,
    
    -- Payment and fare info
    payment_type NUMERIC,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    ehail_fee FLOAT,
    trip_type FLOAT,

    -- Vendor / Rate info (Yellow/Green)
    vendor_id NUMERIC,
    ratecode_id FLOAT,
    store_and_fwd_flag VARCHAR(5),

    -- Location info
    pu_location_id NUMERIC,
    do_location_id NUMERIC,

    -- FHV / HVFHV specific fields
    hvfhs_license_num VARCHAR(50),
    dispatching_base_num VARCHAR(50),
    originating_base_num VARCHAR(50),
    affiliated_base_number VARCHAR(50),
    trip_miles FLOAT,
    trip_time INT,
    base_passenger_fare FLOAT,
    bcf FLOAT,
    sales_tax FLOAT,
    tips FLOAT,
    driver_pay FLOAT,
    shared_request_flag VARCHAR(5),
    shared_match_flag VARCHAR(5),
    access_a_ride_flag VARCHAR(5),
    wav_request_flag VARCHAR(5),
    wav_match_flag VARCHAR(5),
    sr_flag FLOAT,  
     
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);