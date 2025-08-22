CREATE TABLE cityride_analytics.fact_trip_locations (
    year INT,
    month INT,
    file_type VARCHAR(20),
    pickup_location_id INT,
    dropoff_location_id INT,
    total_trips NUMERIC,
    avg_trip_distance NUMERIC,
    avg_trip_time NUMERIC,
    total_amount NUMERIC,
    PRIMARY KEY (year, month, file_type, pickup_location_id, dropoff_location_id)
);


SELECT 
    year,
    month,
    file_type,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    COUNT(*)::NUMERIC AS total_trips,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_trip_distance,
    ROUND(AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60)::NUMERIC, 2) AS avg_trip_time,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount
FROM cityride_analytics.trip_data
GROUP BY year, month, file_type, pu_location_id, do_location_id
ORDER BY year, month, total_trips desc
limit 10;



INSERT INTO cityride_analytics.fact_trip_locations
(
    year,
    month,
    file_type,
    pickup_location_id,
    dropoff_location_id,
    total_trips,
    avg_trip_distance,
    avg_trip_time,
    total_amount
)
SELECT 
    year,
    month,
    file_type,
    pu_location_id::INT AS pickup_location_id,
    do_location_id::INT AS dropoff_location_id,
    COUNT(*)::NUMERIC AS total_trips,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_trip_distance,
    ROUND(AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60)::NUMERIC, 2) AS avg_trip_time,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount
FROM cityride_analytics.trip_data
WHERE pu_location_id::TEXT != 'NaN'
  AND do_location_id::TEXT != 'NaN'
GROUP BY year, month, file_type, pu_location_id, do_location_id;

--dag query
INSERT INTO cityride_analytics.fact_trip_locations
(year, month, file_type, pickup_location_id, dropoff_location_id, total_trips, avg_trip_distance, avg_trip_time, total_amount)
SELECT 
    year,
    month,
    file_type,
    pu_location_id::INT AS pickup_location_id,
    do_location_id::INT AS dropoff_location_id,
    COUNT(*)::NUMERIC AS total_trips,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_trip_distance,
    ROUND(AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60)::NUMERIC, 2) AS avg_trip_time,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount
FROM cityride_analytics.trip_data
WHERE year = {{ params.year }}
  AND month = {{ params.month }}
GROUP BY year, month, file_type, pu_location_id, do_location_id
ON CONFLICT (year, month, file_type, pickup_location_id, dropoff_location_id) DO UPDATE
SET total_trips = EXCLUDED.total_trips,
    avg_trip_distance = EXCLUDED.avg_trip_distance,
    avg_trip_time = EXCLUDED.avg_trip_time,
    total_amount = EXCLUDED.total_amount;

