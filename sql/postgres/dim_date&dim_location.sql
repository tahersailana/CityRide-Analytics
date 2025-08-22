CREATE TABLE cityride_analytics.dim_date (
    date_id DATE PRIMARY KEY,        -- actual date
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,       -- 1=Monday, 7=Sunday
    day_name VARCHAR(10),
    month_name VARCHAR(20),
    quarter INT,
    is_weekend BOOLEAN
);

INSERT INTO cityride_analytics.dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter, is_weekend)
SELECT
    d::DATE AS date_id,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(ISODOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
        (SELECT MIN(make_date(year, month, 1)) FROM cityride_analytics.fact_trip_summary),
        CURRENT_DATE,
        INTERVAL '1 day'
     ) AS d;
     

CREATE TABLE cityride_analytics.dim_location (
    location_id INT PRIMARY KEY,
    location_type VARCHAR(20),    -- e.g., 'pickup', 'dropoff'
    borough VARCHAR(50) DEFAULT NULL, -- optional, if you have mapping of IDs to borough
    zone VARCHAR(50) DEFAULT NULL,    -- optional, if you have mapping of IDs to zones
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pickup locations
INSERT INTO cityride_analytics.dim_location (location_id, location_type)
SELECT DISTINCT pickup_location_id, 'pickup'
FROM cityride_analytics.fact_trip_locations
WHERE pickup_location_id IS NOT NULL
ON CONFLICT (location_id) DO NOTHING;

-- Dropoff locations
	INSERT INTO cityride_analytics.dim_location (location_id, location_type)
	SELECT DISTINCT dropoff_location_id, 'dropoff'
	FROM cityride_analytics.fact_trip_locations
	WHERE dropoff_location_id IS NOT NULL
	ON CONFLICT (location_id) DO NOTHING;