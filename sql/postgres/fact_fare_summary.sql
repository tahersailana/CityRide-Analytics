CREATE TABLE cityride_analytics.fact_fare_summary (
    year INT,
    month INT,
    file_type VARCHAR(20), -- yellow, green, fhv
    total_fare NUMERIC,
    total_tips NUMERIC,
    total_tolls NUMERIC,
    total_surcharges NUMERIC,
    total_amount NUMERIC,
    PRIMARY KEY (year, month, file_type)
);

SELECT 
    year,
    month,
    file_type,
    ROUND(SUM(fare_amount)::NUMERIC, 2) AS total_fare,
    ROUND(SUM(tip_amount)::NUMERIC, 2) AS total_tips,
    ROUND(SUM(tolls_amount)::NUMERIC, 2) AS total_tolls,
    ROUND(SUM(improvement_surcharge + mta_tax + congestion_surcharge + airport_fee)::NUMERIC, 2) AS total_surcharges,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount
FROM cityride_analytics.trip_data
GROUP BY year, month, file_type
ORDER BY year, month, file_type
LIMIT 20;

INSERT INTO cityride_analytics.fact_fare_summary
(year, month, file_type, total_fare, total_tips, total_tolls, total_surcharges, total_amount)
SELECT 
    year,
    month,
    file_type,
    ROUND(SUM(fare_amount)::NUMERIC, 2) AS total_fare,
    ROUND(SUM(tip_amount)::NUMERIC, 2) AS total_tips,
    ROUND(SUM(tolls_amount)::NUMERIC, 2) AS total_tolls,
    ROUND(SUM(improvement_surcharge + mta_tax + congestion_surcharge + airport_fee)::NUMERIC, 2) AS total_surcharges,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount
FROM cityride_analytics.trip_data
GROUP BY year, month, file_type;