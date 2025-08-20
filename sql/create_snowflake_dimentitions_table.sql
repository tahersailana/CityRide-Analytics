-- =====================================================
-- NYC TLC TRIP DATA - STAR SCHEMA
-- =====================================================

-- Use your preferred schema
-- USE SCHEMA analytics;

-- Optional: Set up schema if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS analytics;

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- Date Dimension
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_date (
    date_key INT PRIMARY KEY,                    -- YYYYMMDD format
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,                    -- 1=Sunday, 7=Saturday
    day_name VARCHAR(10) NOT NULL,               -- Monday, Tuesday, etc.
    month_name VARCHAR(10) NOT NULL,             -- January, February, etc.
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    week_of_year INT NOT NULL,
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);

-- Time Dimension
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_time (
    time_key INT PRIMARY KEY,                    -- HHMM format (0000-2359)
    hour INT NOT NULL,                           -- 0-23
    minute INT NOT NULL,                         -- 0-59
    hour_minute VARCHAR(5) NOT NULL,             -- HH:MM format
    time_period VARCHAR(20) NOT NULL,            -- Early Morning, Morning, Afternoon, Evening, Night
    rush_hour_flag BOOLEAN NOT NULL,             -- True during rush hours
    business_hours_flag BOOLEAN NOT NULL,        -- True during business hours (9AM-5PM)
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);

-- Location Dimension (based on TLC Zone Lookup)
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_location (
    location_id INT PRIMARY KEY,
    zone_name VARCHAR(100),
    borough VARCHAR(50),
    service_zone VARCHAR(50),                    -- Yellow, Green, FHV
    location_type VARCHAR(50),                   -- Airport, EWR, Boro Zone, etc.
    is_airport BOOLEAN DEFAULT FALSE,
    is_manhattan BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);


-- Vendor Dimension
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL,
    vendor_description VARCHAR(200),
    vendor_category VARCHAR(50),                 -- Technology Provider, etc.
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);

-- Payment Type Dimension
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_method VARCHAR(50) NOT NULL,         -- Credit Card, Cash, No Charge, Dispute, Unknown, Voided Trip
    payment_category VARCHAR(30) NOT NULL,       -- Electronic, Cash, Other
    is_cashless BOOLEAN NOT NULL,
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);

-- Rate Code Dimension
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_rate_code (
    ratecode_id INT PRIMARY KEY,
    rate_description VARCHAR(100) NOT NULL,      -- Standard, JFK, Newark, Nassau/Westchester, Negotiated, Group Ride
    rate_category VARCHAR(50) NOT NULL,          -- Standard, Airport, Outer Borough, Special
    is_airport_rate BOOLEAN DEFAULT FALSE,
    multiplier DECIMAL(3,2) DEFAULT 1.0,         -- Rate multiplier if applicable
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);

-- Trip Type Dimension
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_trip_type (
    trip_type_id INT PRIMARY KEY,
    trip_description VARCHAR(50) NOT NULL,       -- Street-hail, Dispatch
    trip_category VARCHAR(30) NOT NULL,          -- Hail, Dispatch
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);

-- File Source Dimension
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_file_source (
    file_type VARCHAR(20) PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,           -- Yellow Taxi, Green Taxi, For-Hire Vehicle
    service_description VARCHAR(200),
    service_category VARCHAR(30) NOT NULL,       -- Taxi, FHV
    regulation_type VARCHAR(50),                 -- Medallion, Street Hail Livery, FHV
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);

-- Store and Forward Flag Dimension (Small lookup table)
CREATE OR REPLACE TABLE CITYRIDE_ANALYTICS.dim_store_fwd_flag (
    store_fwd_flag CHAR(1) PRIMARY KEY,
    flag_description VARCHAR(100) NOT NULL,      -- Store and Forward, Not Store and Forward
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);



-- =====================================================
-- DIMENSION TABLE POPULATION - INSERT STATEMENTS
-- =====================================================

-- =====================================================
-- 1. DIM_DATE - Generate date range (2009-2030)
-- =====================================================
-- Note: This generates a large number of records, adjust date range as needed

INSERT INTO CITYRIDE_ANALYTICS.dim_date (
    date_key,
    full_date,
    year,
    month,
    day,
    quarter,
    day_of_week,
    day_name,
    month_name,
    is_weekend,
    is_holiday,
    week_of_year
)
WITH date_spine AS (
  SELECT 
    DATEADD(day, SEQ4(), '2009-01-01'::DATE) AS calendar_date
  FROM TABLE(GENERATOR(ROWCOUNT => 8036)) -- Roughly 22 years of dates
  WHERE calendar_date <= '2030-12-31'
)
SELECT 
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD')) AS date_key,
    calendar_date AS full_date,
    YEAR(calendar_date) AS year,
    MONTH(calendar_date) AS month,
    DAY(calendar_date) AS day,
    QUARTER(calendar_date) AS quarter,
    DAYOFWEEK(calendar_date) AS day_of_week,
    DAYNAME(calendar_date) AS day_name,
    MONTHNAME(calendar_date) AS month_name,
    CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday, -- You can update this later with actual holidays
    WEEKOFYEAR(calendar_date) AS week_of_year
FROM date_spine;

-- =====================================================
-- 2. DIM_TIME - Generate all time combinations
-- =====================================================
INSERT INTO CITYRIDE_ANALYTICS.dim_time (
    time_key,
    hour,
    minute,
    hour_minute,
    time_period,
    rush_hour_flag,
    business_hours_flag
)
WITH time_spine AS (
  SELECT 
    SEQ4() AS minute_of_day
  FROM TABLE(GENERATOR(ROWCOUNT => 1440)) -- 24 hours * 60 minutes
  WHERE minute_of_day < 1440
)
SELECT 
    (FLOOR(minute_of_day / 60) * 100) + (minute_of_day % 60) AS time_key,
    FLOOR(minute_of_day / 60) AS hour,
    minute_of_day % 60 AS minute,
    LPAD(FLOOR(minute_of_day / 60), 2, '0') || ':' || LPAD(minute_of_day % 60, 2, '0') AS hour_minute,
    CASE 
        WHEN FLOOR(minute_of_day / 60) BETWEEN 5 AND 8 THEN 'Early Morning'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 9 AND 11 THEN 'Morning'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS time_period,
    CASE 
        WHEN FLOOR(minute_of_day / 60) BETWEEN 7 AND 9 THEN TRUE  -- Morning rush
        WHEN FLOOR(minute_of_day / 60) BETWEEN 17 AND 19 THEN TRUE -- Evening rush
        ELSE FALSE
    END AS rush_hour_flag,
    CASE 
        WHEN FLOOR(minute_of_day / 60) BETWEEN 9 AND 17 THEN TRUE
        ELSE FALSE
    END AS business_hours_flag
FROM time_spine;

-- =====================================================
-- 3. DIM_LOCATION
-- =====================================================
-- Minimal location records to get started
INSERT INTO CITYRIDE_ANALYTICS.dim_location (location_id, zone_name, borough, service_zone, location_type, is_airport, is_manhattan) VALUES
(0, 'Unknown/Missing', 'Unknown', 'Unknown', 'Unknown', FALSE, FALSE),
(1, 'EWR Airport', 'EWR', 'EWR', 'Airport', TRUE, FALSE),
(2, 'Queens', 'Queens', 'Yellow', 'Standard', FALSE, FALSE),
(3, 'Brooklyn', 'Brooklyn', 'Green', 'Standard', FALSE, FALSE),
(4, 'Manhattan', 'Manhattan', 'Yellow', 'Standard', FALSE, TRUE),
(5, 'Bronx', 'Bronx', 'Green', 'Standard', FALSE, FALSE),
(6, 'Staten Island', 'Staten Island', 'Green', 'Standard', FALSE, FALSE);

-- =====================================================
-- 4. DIM_VENDOR (Common NYC TLC Vendors)
-- =====================================================
INSERT INTO CITYRIDE_ANALYTICS.dim_vendor (vendor_id, vendor_name, vendor_description, vendor_category, is_active) VALUES
(1, 'Creative Mobile Technologies', 'Taxi technology and payment processing provider', 'Technology Provider', TRUE),
(2, 'VeriFone Inc', 'Taxi technology and payment processing provider', 'Technology Provider', TRUE),
(3, 'Other/Unknown Vendor', 'Other or unspecified technology vendor', 'Other', TRUE),
(0, 'Not Specified', 'Vendor information not provided', 'Unknown', TRUE);


-- =====================================================
-- 5. DIM_PAYMENT_TYPE
-- =====================================================
INSERT INTO CITYRIDE_ANALYTICS.dim_payment_type (payment_type_id, payment_method, payment_category, is_cashless) VALUES
(1, 'Credit Card', 'Electronic', TRUE),
(2, 'Cash', 'Cash', FALSE),
(3, 'No Charge', 'Other', TRUE),
(4, 'Dispute', 'Other', TRUE),
(5, 'Unknown', 'Other', TRUE),
(6, 'Voided Trip', 'Other', TRUE),
(0, 'Not Specified', 'Other', TRUE);

-- =====================================================
-- 6. DIM_RATE_CODE
-- =====================================================
INSERT INTO CITYRIDE_ANALYTICS.dim_rate_code (ratecode_id, rate_description, rate_category, is_airport_rate, multiplier) VALUES
(1, 'Standard Rate', 'Standard', FALSE, 1.0),
(2, 'JFK Airport', 'Airport', TRUE, 1.0),
(3, 'Newark Airport', 'Airport', TRUE, 1.0),
(4, 'Nassau or Westchester', 'Outer Borough', FALSE, 1.0),
(5, 'Negotiated Fare', 'Special', FALSE, 1.0),
(6, 'Group Ride', 'Special', FALSE, 1.0),
(99, 'Unknown', 'Other', FALSE, 1.0),
(0, 'Not Specified', 'Other', FALSE, 1.0);

-- =====================================================
-- 7. DIM_TRIP_TYPE
-- =====================================================
INSERT INTO CITYRIDE_ANALYTICS.dim_trip_type (trip_type_id, trip_description, trip_category) VALUES
(1, 'Street Hail', 'Hail'),
(2, 'Dispatch', 'Dispatch'),
(0, 'Not Specified', 'Unknown');

-- =====================================================
-- 8. DIM_FILE_SOURCE
-- =====================================================
INSERT INTO CITYRIDE_ANALYTICS.dim_file_source (file_type, service_name, service_description, service_category, regulation_type) VALUES
('yellow', 'Yellow Taxi', 'Traditional yellow medallion taxis operating primarily in Manhattan and airports', 'Taxi', 'Medallion'),
('green', 'Green Taxi', 'Street hail livery vehicles (Boro Taxis) operating outside Manhattan core', 'Taxi', 'Street Hail Livery'),
('fhv', 'For-Hire Vehicle', 'App-based ride services, livery vehicles, and other for-hire transportation', 'FHV', 'For-Hire Vehicle');

-- =====================================================
-- 9. DIM_STORE_FWD_FLAG
-- =====================================================
INSERT INTO CITYRIDE_ANALYTICS.dim_store_fwd_flag (store_fwd_flag, flag_description) VALUES
('Y', 'Store and Forward - Trip record held in vehicle memory before sending to vendor'),
('N', 'Not Store and Forward - Trip record sent immediately to vendor'),
(' ', 'Not Specified');

