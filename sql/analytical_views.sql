-- ============================================================
-- ADVANCED ANALYTICAL VIEWS FOR NYC GREEN TAXI DATA
-- ============================================================
-- These views provide sophisticated pre-aggregated analytics
-- for executive dashboards and business intelligence
-- Created for: DSAI5102 Data Architecture & Engineering Project
-- ============================================================

-- Create schema for analytical views
CREATE SCHEMA IF NOT EXISTS nyc_taxi;
GRANT ALL ON SCHEMA nyc_taxi TO demo;

-- ============================================================
-- 1. TRIPS BY TIME OF DAY (Morning/Afternoon/Evening/Night)
-- Business Insight: Understand demand patterns across day parts
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_trips_by_time_of_day AS
SELECT 
    time_of_day,
    COUNT(*) as trip_count,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(trip_duration_min)::numeric, 2) as avg_duration_min,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue
FROM public.fact_trips
GROUP BY time_of_day
ORDER BY 
    CASE time_of_day 
        WHEN 'Morning' THEN 1 
        WHEN 'Afternoon' THEN 2 
        WHEN 'Evening' THEN 3 
        WHEN 'Night' THEN 4 
    END;

-- ============================================================
-- 2. WEEKEND VS WEEKDAY COMPARISON
-- Business Insight: Compare operational patterns and revenue
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_weekend_weekday_comparison AS
SELECT 
    day_type,
    COUNT(*) as trip_count,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(tip_percentage)::numeric, 2) as avg_tip_pct,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue
FROM public.fact_trips
GROUP BY day_type;

-- ============================================================
-- 3. DISTANCE CATEGORY DISTRIBUTION
-- Business Insight: Trip length segmentation analysis
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_distance_category_dist AS
SELECT 
    distance_category,
    COUNT(*) as trip_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_duration_min)::numeric, 2) as avg_duration
FROM public.fact_trips
GROUP BY distance_category
ORDER BY 
    CASE distance_category 
        WHEN 'Short (<1mi)' THEN 1 
        WHEN 'Medium (1-5mi)' THEN 2 
        WHEN 'Long (5-10mi)' THEN 3 
        WHEN 'Very Long (>10mi)' THEN 4 
    END;

-- ============================================================
-- 4. FARE CATEGORY BREAKDOWN
-- Business Insight: Revenue tier analysis
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_fare_category_breakdown AS
SELECT 
    fare_category,
    COUNT(*) as trip_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(tip_percentage)::numeric, 2) as avg_tip_pct
FROM public.fact_trips
GROUP BY fare_category
ORDER BY 
    CASE fare_category 
        WHEN 'Budget (<$10)' THEN 1 
        WHEN 'Standard ($10-$25)' THEN 2 
        WHEN 'Premium ($25-$50)' THEN 3 
        WHEN 'Luxury (>$50)' THEN 4 
    END;

-- ============================================================
-- 5. AVERAGE SPEED BY HOUR (Traffic Analysis)
-- Business Insight: Identify peak congestion times
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_avg_speed_by_hour AS
SELECT 
    trip_hour,
    COUNT(*) as trip_count,
    ROUND(AVG(avg_speed_mph)::numeric, 2) as avg_speed_mph,
    ROUND(AVG(trip_duration_min)::numeric, 2) as avg_duration_min
FROM public.fact_trips
WHERE avg_speed_mph IS NOT NULL
GROUP BY trip_hour
ORDER BY trip_hour;

-- ============================================================
-- 6. TIP PERCENTAGE BY PAYMENT TYPE
-- Business Insight: Payment method impact on tips
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_tip_by_payment AS
SELECT 
    payment_type_name,
    COUNT(*) as trip_count,
    ROUND(AVG(tip_percentage)::numeric, 2) as avg_tip_pct,
    ROUND(AVG(tip_amount)::numeric, 2) as avg_tip_amount,
    ROUND(SUM(tip_amount)::numeric, 2) as total_tips
FROM public.fact_trips
GROUP BY payment_type_name
ORDER BY avg_tip_pct DESC NULLS LAST;

-- ============================================================
-- 7. VENDOR PERFORMANCE COMPARISON
-- Business Insight: Compare taxi vendor efficiency
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_vendor_performance AS
SELECT 
    vendor_name,
    COUNT(*) as total_trips,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(avg_speed_mph)::numeric, 2) as avg_speed_mph,
    ROUND(AVG(tip_percentage)::numeric, 2) as avg_tip_pct,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue
FROM public.fact_trips
GROUP BY vendor_name;

-- ============================================================
-- 8. FARE EFFICIENCY ANALYSIS
-- Business Insight: Revenue per mile/minute by time & day type
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_fare_efficiency AS
SELECT 
    time_of_day,
    day_type,
    ROUND(AVG(fare_per_mile)::numeric, 2) as avg_fare_per_mile,
    ROUND(AVG(fare_per_minute)::numeric, 2) as avg_fare_per_minute,
    COUNT(*) as trip_count
FROM public.fact_trips
WHERE fare_per_mile IS NOT NULL AND fare_per_minute IS NOT NULL
GROUP BY time_of_day, day_type
ORDER BY time_of_day, day_type;

-- ============================================================
-- 9. HOURLY REVENUE ANALYSIS
-- Business Insight: Revenue patterns throughout the day
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_hourly_revenue AS
SELECT 
    trip_hour,
    COUNT(*) as trip_count,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(SUM(tip_amount)::numeric, 2) as total_tips
FROM public.fact_trips
GROUP BY trip_hour
ORDER BY trip_hour;

-- ============================================================
-- 10. DAILY SUMMARY DASHBOARD
-- Business Insight: Day-by-day operational overview
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_daily_summary AS
SELECT 
    trip_day,
    COUNT(*) as total_trips,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(trip_duration_min)::numeric, 2) as avg_duration_min
FROM public.fact_trips
GROUP BY trip_day
ORDER BY trip_day;

-- ============================================================
-- KPI SUMMARY VIEW (Executive Dashboard)
-- Business Insight: High-level business metrics
-- ============================================================
CREATE OR REPLACE VIEW nyc_taxi.v_kpi_summary AS
SELECT 
    COUNT(*) as total_trips,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance_miles,
    ROUND(AVG(trip_duration_min)::numeric, 2) as avg_duration_minutes,
    ROUND(AVG(avg_speed_mph)::numeric, 2) as avg_speed_mph,
    ROUND(AVG(tip_percentage)::numeric, 2) as avg_tip_percentage,
    ROUND(SUM(tip_amount)::numeric, 2) as total_tips
FROM public.fact_trips;

-- Grant access to all views
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi TO demo;
