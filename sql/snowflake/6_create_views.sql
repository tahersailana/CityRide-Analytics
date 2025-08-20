-- 1. Core Analytics Views

-- vw_trip_analytics: Comprehensive view with all dimensions and calculated metrics
-- vw_revenue_analytics: Financial performance analysis
-- vw_location_performance: Route and location-based metrics
-- vw_time_patterns: Temporal analysis (hourly, daily patterns)
-- vw_payment_behavior: Payment method insights and tipping patterns

-- 2. Dashboard-Specific Views

-- vw_executive_kpis: High-level KPIs with period-over-period comparisons
-- vw_service_comparison: Compare Yellow, Green, and FHV services
-- vw_hourly_demand_heatmap: Demand patterns by hour and day
-- vw_top_routes: Most popular pickup-dropoff combinations
-- vw_monthly_trends: Time series analysis with growth metrics
-- vw_airport_traffic: Airport-specific trip analysis
-- vw_vendor_performance: Vendor market share and performance
-- vw_rush_hour_analysis: Peak vs off-peak comparisons


-- =====================================================
-- NYC TLC ANALYTICS VIEWS & DASHBOARD QUERIES
-- =====================================================

-- =====================================================
-- CORE ANALYTICS VIEWS
-- =====================================================

-- 1. Comprehensive Trip Analytics View
CREATE OR REPLACE VIEW vw_trip_analytics AS
SELECT 
    -- Trip identifiers
    f.trip_id,
    f.file_type,
    fs.service_name,
    fs.service_category,
    
    -- Temporal dimensions
    pd.full_date as pickup_date,
    pd.year as pickup_year,
    pd.month as pickup_month,
    pd.month_name as pickup_month_name,
    pd.quarter as pickup_quarter,
    pd.day_of_week as pickup_day_of_week,
    pd.day_name as pickup_day_name,
    pd.is_weekend as pickup_is_weekend,
    pd.week_of_year as pickup_week,
    
    pt.hour as pickup_hour,
    pt.hour_minute as pickup_time,
    pt.time_period as pickup_time_period,
    pt.rush_hour_flag as pickup_is_rush_hour,
    pt.business_hours_flag as pickup_is_business_hours,
    
    -- Location dimensions
    pl.location_id as pickup_location_id,
    pl.zone_name as pickup_zone,
    pl.borough as pickup_borough,
    pl.is_airport as pickup_is_airport,
    pl.is_manhattan as pickup_is_manhattan,
    
    dl.location_id as dropoff_location_id,
    dl.zone_name as dropoff_zone,
    dl.borough as dropoff_borough,
    dl.is_airport as dropoff_is_airport,
    dl.is_manhattan as dropoff_is_manhattan,
    
    -- Vendor and payment
    v.vendor_name,
    v.vendor_category,
    pm.payment_method,
    pm.payment_category,
    pm.is_cashless,
    
    -- Rate and trip type
    rc.rate_description,
    rc.rate_category,
    rc.is_airport_rate,
    tt.trip_description,
    sf.flag_description as store_forward_status,
    
    -- Measures
    f.passenger_count,
    f.trip_distance,
    f.trip_duration_minutes,
    
    -- Financial measures
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.congestion_surcharge,
    f.airport_fee,
    f.total_amount,
    
    -- Calculated measures
    CASE 
        WHEN f.fare_amount > 0 THEN ROUND((f.tip_amount / f.fare_amount) * 100, 2)
        ELSE 0 
    END as tip_percentage,
    
    CASE 
        WHEN f.trip_duration_minutes > 0 THEN ROUND(f.trip_distance / (f.trip_duration_minutes / 60.0), 2)
        ELSE 0 
    END as avg_speed_mph,
    
    CASE 
        WHEN f.trip_distance > 0 THEN ROUND(f.total_amount / f.trip_distance, 2)
        ELSE 0 
    END as revenue_per_mile,
    
    -- FHV specific
    f.dispatching_base_num,
    f.affiliated_base_number,
    f.sr_flag
    
FROM fact_trips f
LEFT JOIN dim_date pd ON f.pickup_date_key = pd.date_key
LEFT JOIN dim_date dd ON f.dropoff_date_key = dd.date_key
LEFT JOIN dim_time pt ON f.pickup_time_key = pt.time_key
LEFT JOIN dim_time dt ON f.dropoff_time_key = dt.time_key
LEFT JOIN dim_location pl ON f.pickup_location_id = pl.location_id
LEFT JOIN dim_location dl ON f.dropoff_location_id = dl.location_id
LEFT JOIN dim_file_source fs ON f.file_type = fs.file_type
LEFT JOIN dim_vendor v ON f.vendor_id = v.vendor_id
LEFT JOIN dim_payment_type pm ON f.payment_type_id = pm.payment_type_id
LEFT JOIN dim_rate_code rc ON f.ratecode_id = rc.ratecode_id
LEFT JOIN dim_trip_type tt ON f.trip_type_id = tt.trip_type_id
LEFT JOIN dim_store_fwd_flag sf ON f.store_fwd_flag = sf.store_fwd_flag;

-- 2. Revenue Analytics View
CREATE OR REPLACE VIEW vw_revenue_analytics AS
SELECT 
    pd.full_date,
    pd.year,
    pd.month,
    pd.month_name,
    pd.quarter,
    pd.is_weekend,
    fs.service_name,
    v.vendor_name,
    pm.payment_method,
    
    COUNT(*) as trip_count,
    SUM(f.total_amount) as total_revenue,
    SUM(f.fare_amount) as total_fares,
    SUM(f.tip_amount) as total_tips,
    SUM(f.tolls_amount) as total_tolls,
    SUM(f.congestion_surcharge) as total_congestion_charges,
    
    AVG(f.total_amount) as avg_trip_revenue,
    AVG(f.fare_amount) as avg_fare,
    AVG(CASE WHEN f.fare_amount > 0 THEN (f.tip_amount / f.fare_amount) * 100 ELSE 0 END) as avg_tip_percentage,
    
    SUM(f.passenger_count) as total_passengers,
    AVG(f.trip_distance) as avg_trip_distance,
    AVG(f.trip_duration_minutes) as avg_trip_duration
    
FROM fact_trips f
JOIN dim_date pd ON f.pickup_date_key = pd.date_key
JOIN dim_file_source fs ON f.file_type = fs.file_type
LEFT JOIN dim_vendor v ON f.vendor_id = v.vendor_id
LEFT JOIN dim_payment_type pm ON f.payment_type_id = pm.payment_type_id
GROUP BY 1,2,3,4,5,6,7,8,9;

-- 3. Location Performance View
CREATE OR REPLACE VIEW vw_location_performance AS
SELECT 
    pl.borough as pickup_borough,
    pl.zone_name as pickup_zone,
    dl.borough as dropoff_borough,
    dl.zone_name as dropoff_zone,
    fs.service_name,
    
    COUNT(*) as trip_count,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.total_amount) as avg_revenue,
    SUM(f.total_amount) as total_revenue,
    
    AVG(CASE WHEN f.trip_duration_minutes > 0 
        THEN f.trip_distance / (f.trip_duration_minutes / 60.0) 
        ELSE 0 END) as avg_speed_mph,
    
    COUNT(DISTINCT f.pickup_date_key) as active_days
    
FROM fact_trips f
JOIN dim_location pl ON f.pickup_location_id = pl.location_id
JOIN dim_location dl ON f.dropoff_location_id = dl.location_id
JOIN dim_file_source fs ON f.file_type = fs.file_type
GROUP BY 1,2,3,4,5;

-- 4. Time Pattern Analysis View
CREATE OR REPLACE VIEW vw_time_patterns AS
SELECT 
    pt.hour as pickup_hour,
    pt.time_period,
    pt.rush_hour_flag,
    pd.day_name,
    pd.is_weekend,
    fs.service_name,
    
    COUNT(*) as trip_count,
    AVG(f.total_amount) as avg_revenue,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.passenger_count) as avg_passengers,
    
    SUM(f.total_amount) as total_revenue,
    
    AVG(CASE WHEN f.trip_duration_minutes > 0 
        THEN f.trip_distance / (f.trip_duration_minutes / 60.0) 
        ELSE 0 END) as avg_speed_mph
    
FROM fact_trips f
JOIN dim_time pt ON f.pickup_time_key = pt.time_key
JOIN dim_date pd ON f.pickup_date_key = pd.date_key
JOIN dim_file_source fs ON f.file_type = fs.file_type
GROUP BY 1,2,3,4,5,6;

-- 5. Payment Behavior View
CREATE OR REPLACE VIEW vw_payment_behavior AS
SELECT 
    pm.payment_method,
    pm.payment_category,
    pm.is_cashless,
    fs.service_name,
    pd.year,
    pd.month_name,
    
    COUNT(*) as trip_count,
    AVG(f.total_amount) as avg_total_amount,
    AVG(f.tip_amount) as avg_tip_amount,
    
    AVG(CASE WHEN f.fare_amount > 0 
        THEN (f.tip_amount / f.fare_amount) * 100 
        ELSE 0 END) as avg_tip_percentage,
    
    SUM(f.total_amount) as total_revenue,
    SUM(f.tip_amount) as total_tips,
    
    COUNT(CASE WHEN f.tip_amount > 0 THEN 1 END) as trips_with_tips,
    
    ROUND(100.0 * COUNT(CASE WHEN f.tip_amount > 0 THEN 1 END) / COUNT(*), 2) as tip_frequency_percentage
    
FROM fact_trips f
JOIN dim_payment_type pm ON f.payment_type_id = pm.payment_type_id
JOIN dim_file_source fs ON f.file_type = fs.file_type
JOIN dim_date pd ON f.pickup_date_key = pd.date_key
GROUP BY 1,2,3,4,5,6;

-- =====================================================
-- DASHBOARD SPECIFIC QUERIES
-- =====================================================
-- 1. Executive Dashboard - Key Metrics (Current Period vs Previous)
CREATE OR REPLACE VIEW vw_executive_kpis AS
WITH current_period AS (
    SELECT 
        COUNT(*) as total_trips,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_trip_value,
        SUM(passenger_count) as total_passengers,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration_minutes) as avg_duration
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    WHERE d.year = YEAR(CURRENT_DATE()) 
      AND d.month = MONTH(CURRENT_DATE())
),
previous_period AS (
    SELECT 
        COUNT(*) as total_trips,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_trip_value,
        SUM(passenger_count) as total_passengers,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration_minutes) as avg_duration
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    WHERE d.year = YEAR(DATEADD('month', -1, CURRENT_DATE()))
      AND d.month = MONTH(DATEADD('month', -1, CURRENT_DATE()))
)
SELECT 
    'Current Month' as period,
    c.total_trips,
    c.total_revenue,
    c.avg_trip_value,
    c.total_passengers,
    c.avg_distance,
    c.avg_duration,
    ROUND(((c.total_trips - p.total_trips) / NULLIF(p.total_trips, 0)) * 100, 2) as trips_growth_pct,
    ROUND(((c.total_revenue - p.total_revenue) / NULLIF(p.total_revenue, 0)) * 100, 2) as revenue_growth_pct
FROM current_period c, previous_period p;

-- 2. Service Type Comparison Dashboard
CREATE OR REPLACE VIEW vw_service_comparison AS
SELECT 
    fs.service_name,
    fs.service_category,
    COUNT(*) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_revenue_per_trip,
    AVG(f.trip_distance) as avg_trip_distance,
    AVG(f.trip_duration_minutes) as avg_trip_duration,
    AVG(f.passenger_count) as avg_passengers,
    
    AVG(CASE WHEN f.fare_amount > 0 
        THEN (f.tip_amount / f.fare_amount) * 100 
        ELSE 0 END) as avg_tip_percentage,
    
    SUM(CASE WHEN pm.is_cashless THEN 1 ELSE 0 END) as cashless_trips,
    ROUND(100.0 * SUM(CASE WHEN pm.is_cashless THEN 1 ELSE 0 END) / COUNT(*), 2) as cashless_percentage,
    
    COUNT(DISTINCT f.pickup_date_key) as operating_days,
    COUNT(DISTINCT f.vendor_id) as active_vendors
    
FROM fact_trips f
JOIN dim_file_source fs ON f.file_type = fs.file_type
LEFT JOIN dim_payment_type pm ON f.payment_type_id = pm.payment_type_id
GROUP BY 1,2;

-- 3. Hourly Demand Heatmap Data
CREATE OR REPLACE VIEW vw_hourly_demand_heatmap AS
SELECT 
    d.day_name,
    d.day_of_week,
    t.hour,
    fs.service_name,
    COUNT(*) as trip_count,
    AVG(f.total_amount) as avg_revenue,
    
    -- Normalize for heatmap (0-100 scale)
    ROUND(100.0 * COUNT(*) / MAX(COUNT(*)) OVER (PARTITION BY fs.service_name), 2) as demand_intensity
    
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
JOIN dim_time t ON f.pickup_time_key = t.time_key
JOIN dim_file_source fs ON f.file_type = fs.file_type
GROUP BY 1,2,3,4
ORDER BY d.day_of_week, t.hour;

-- 4. Top Routes Dashboard
CREATE OR REPLACE VIEW vw_top_routes AS
SELECT 
    pl.borough as pickup_borough,
    pl.zone_name as pickup_zone,
    dl.borough as dropoff_borough,
    dl.zone_name as dropoff_zone,
    fs.service_name,
    
    COUNT(*) as trip_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_revenue,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    
    RANK() OVER (PARTITION BY fs.service_name ORDER BY COUNT(*) DESC) as route_rank_by_volume,
    RANK() OVER (PARTITION BY fs.service_name ORDER BY SUM(f.total_amount) DESC) as route_rank_by_revenue
    
FROM fact_trips f
JOIN dim_location pl ON f.pickup_location_id = pl.location_id
JOIN dim_location dl ON f.dropoff_location_id = dl.location_id
JOIN dim_file_source fs ON f.file_type = fs.file_type
WHERE pl.location_id != 0 AND dl.location_id != 0  -- Exclude unknown locations
GROUP BY 1,2,3,4,5
HAVING COUNT(*) > 100  -- Filter for significant routes
ORDER BY trip_count DESC
LIMIT 100;

-- 5. Monthly Trend Analysis
CREATE OR REPLACE VIEW vw_monthly_trends AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    fs.service_name,
    
    COUNT(*) as trip_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_revenue_per_trip,
    SUM(f.passenger_count) as total_passengers,
    AVG(f.trip_distance) as avg_distance,
    
    -- Month-over-month calculations
    LAG(COUNT(*)) OVER (PARTITION BY fs.service_name ORDER BY d.year, d.month) as prev_month_trips,
    LAG(SUM(f.total_amount)) OVER (PARTITION BY fs.service_name ORDER BY d.year, d.month) as prev_month_revenue,
    
    ROUND(100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY fs.service_name ORDER BY d.year, d.month)) / 
          NULLIF(LAG(COUNT(*)) OVER (PARTITION BY fs.service_name ORDER BY d.year, d.month), 0), 2) as trips_mom_growth,
    
    ROUND(100.0 * (SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (PARTITION BY fs.service_name ORDER BY d.year, d.month)) / 
          NULLIF(LAG(SUM(f.total_amount)) OVER (PARTITION BY fs.service_name ORDER BY d.year, d.month), 0), 2) as revenue_mom_growth
    
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
JOIN dim_file_source fs ON f.file_type = fs.file_type
GROUP BY 1,2,3,4
ORDER BY 1,2,4;

-- 6. Airport Traffic Analysis
CREATE OR REPLACE VIEW vw_airport_traffic AS
SELECT 
    CASE 
        WHEN pl.is_airport THEN 'From Airport'
        WHEN dl.is_airport THEN 'To Airport'
        ELSE 'Non-Airport'
    END as trip_type,
    
    CASE 
        WHEN pl.is_airport THEN pl.zone_name
        WHEN dl.is_airport THEN dl.zone_name
        ELSE 'N/A'
    END as airport_name,
    
    fs.service_name,
    d.year,
    d.month_name,
    t.time_period,
    
    COUNT(*) as trip_count,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    SUM(f.total_amount) as total_revenue
    
FROM fact_trips f
JOIN dim_location pl ON f.pickup_location_id = pl.location_id
JOIN dim_location dl ON f.dropoff_location_id = dl.location_id
JOIN dim_file_source fs ON f.file_type = fs.file_type
JOIN dim_date d ON f.pickup_date_key = d.date_key
JOIN dim_time t ON f.pickup_time_key = t.time_key
WHERE pl.is_airport = TRUE OR dl.is_airport = TRUE
GROUP BY 1,2,3,4,5,6;

-- 7. Vendor Performance Dashboard
CREATE OR REPLACE VIEW vw_vendor_performance AS
SELECT 
    v.vendor_name,
    v.vendor_category,
    fs.service_name,
    d.year,
    d.month_name,
    
    COUNT(*) as trip_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_revenue_per_trip,
    AVG(f.trip_distance) as avg_trip_distance,
    AVG(f.trip_duration_minutes) as avg_trip_duration,
    
    AVG(CASE WHEN f.fare_amount > 0 
        THEN (f.tip_amount / f.fare_amount) * 100 
        ELSE 0 END) as avg_tip_percentage,
    
    COUNT(DISTINCT f.pickup_date_key) as operating_days,
    COUNT(DISTINCT f.pickup_location_id) as coverage_locations,
    
    -- Market share
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY fs.service_name, d.year, d.month_name), 2) as market_share_pct
    
FROM fact_trips f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
JOIN dim_file_source fs ON f.file_type = fs.file_type
JOIN dim_date d ON f.pickup_date_key = d.date_key
WHERE v.vendor_id != 0  -- Exclude unknown vendors
GROUP BY 1,2,3,4,5;

-- 8. Rush Hour Analysis
CREATE OR REPLACE VIEW vw_rush_hour_analysis AS
SELECT 
    t.rush_hour_flag,
    CASE WHEN t.rush_hour_flag THEN 'Rush Hour' ELSE 'Non-Rush Hour' END as period_type,
    d.is_weekend,
    fs.service_name,
    
    COUNT(*) as trip_count,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    
    AVG(CASE WHEN f.trip_duration_minutes > 0 
        THEN f.trip_distance / (f.trip_duration_minutes / 60.0) 
        ELSE 0 END) as avg_speed_mph,
    
    AVG(f.passenger_count) as avg_passengers,
    
    SUM(f.total_amount) as total_revenue
    
FROM fact_trips f
JOIN dim_time t ON f.pickup_time_key = t.time_key
JOIN dim_date d ON f.pickup_date_key = d.date_key
JOIN dim_file_source fs ON f.file_type = fs.file_type
GROUP BY 1,2,3,4;
