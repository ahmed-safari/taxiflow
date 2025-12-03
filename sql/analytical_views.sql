SET search_path TO nyc_taxi;

-- ======================================================
-- TRIPS PER HOUR
-- ======================================================
CREATE OR REPLACE VIEW trips_by_hour AS
SELECT 
    trip_hour,
    COUNT(*) AS total_trips
FROM fact_trips
GROUP BY trip_hour
ORDER BY trip_hour;

-- ======================================================
-- TRIPS PER WEEKDAY
-- 1 = Sunday, 7 = Saturday (PostgreSQL default)
-- ======================================================
CREATE OR REPLACE VIEW trips_by_weekday AS
SELECT 
    trip_weekday,
    COUNT(*) AS total_trips
FROM fact_trips
GROUP BY trip_weekday
ORDER BY trip_weekday;

-- ======================================================
-- PAYMENT TYPE DISTRIBUTION
-- ======================================================
CREATE OR REPLACE VIEW payment_type_distribution AS
SELECT 
    p.payment_desc,
    COUNT(*) AS total_trips
FROM fact_trips f
JOIN dim_payment_type p
ON f.payment_type = p.payment_type
GROUP BY p.payment_desc
ORDER BY total_trips DESC;

-- ======================================================
-- AVERAGE TRIP DURATION PER HOUR
-- ======================================================
CREATE OR REPLACE VIEW avg_trip_duration_hour AS
SELECT
    trip_hour,
    AVG(trip_duration_min) AS avg_duration_min
FROM fact_trips
GROUP BY trip_hour
ORDER BY trip_hour;
