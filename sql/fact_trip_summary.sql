CREATE TABLE cityride_analytics.fact_trip_summary (
    year INT,
    month INT,
    file_type VARCHAR(20), -- yellow, green, fhv
    total_trips NUMERIC,
    total_passengers NUMERIC,
    avg_trip_distance NUMERIC(10,2),
    avg_trip_time NUMERIC(10,2),
    total_fare NUMERIC(18,2),
    total_tips NUMERIC(18,2),
    total_amount NUMERIC(18,2),
    PRIMARY KEY (year, month, file_type)
);
drop table cityride_analytics.fact_trip_summary

SELECT 
    EXTRACT(YEAR FROM pickup_datetime)::INT AS year,
    EXTRACT(MONTH FROM pickup_datetime)::INT AS month,
    file_type,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60) AS avg_trip_time,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tips,
    SUM(total_amount) AS total_amount
FROM cityride_analytics.trip_data
GROUP BY 1,2,3
ORDER BY year, month, file_type;

INSERT INTO cityride_analytics.fact_trip_summary
(year, month, file_type, total_trips, total_passengers, avg_trip_distance, avg_trip_time, total_fare, total_tips, total_amount)
SELECT 
    year,
    month,
    file_type,
    COUNT(*)::NUMERIC AS total_trips,
    SUM(passenger_count)::NUMERIC AS total_passengers,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_trip_distance,
    ROUND(AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60)::NUMERIC, 2) AS avg_trip_time,
    ROUND(SUM(fare_amount)::NUMERIC, 2) AS total_fare,
    ROUND(SUM(tip_amount)::NUMERIC, 2) AS total_tips,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount
FROM cityride_analytics.trip_data
GROUP BY year, month, file_type;

--dag query
INSERT INTO cityride_analytics.fact_trip_summary
(year, month, file_type, total_trips, total_passengers, avg_trip_distance, avg_trip_time, total_fare, total_tips, total_amount)
SELECT 
    year,
    month,
    file_type,
    COUNT(*)::NUMERIC AS total_trips,
    SUM(passenger_count)::NUMERIC AS total_passengers,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_trip_distance,
    ROUND(AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60)::NUMERIC, 2) AS avg_trip_time,
    ROUND(SUM(fare_amount)::NUMERIC, 2) AS total_fare,
    ROUND(SUM(tip_amount)::NUMERIC, 2) AS total_tips,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount
FROM cityride_analytics.trip_data
WHERE year = {{ params.year }}
  AND month = {{ params.month }}
GROUP BY year, month, file_type
ON CONFLICT (year, month, file_type) DO UPDATE
SET total_trips = EXCLUDED.total_trips,
    total_passengers = EXCLUDED.total_passengers,
    avg_trip_distance = EXCLUDED.avg_trip_distance,
    avg_trip_time = EXCLUDED.avg_trip_time,
    total_fare = EXCLUDED.total_fare,
    total_tips = EXCLUDED.total_tips,
    total_amount = EXCLUDED.total_amount;