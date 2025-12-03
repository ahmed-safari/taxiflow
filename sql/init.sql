-- ======================================================
-- NYC Green Taxi Data Warehouse - Initialization Script
-- This script runs automatically on PostgreSQL startup
-- ======================================================

-- Connect to the sampledb database
\connect sampledb;

-- Create the main schema
CREATE SCHEMA IF NOT EXISTS nyc_taxi;
SET search_path TO nyc_taxi;

-- ======================================================
-- DIMENSION TABLES
-- ======================================================

-- Payment Type Dimension
-- Reference: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
-- 1 = Credit card
-- 2 = Cash
-- 3 = No charge
-- 4 = Dispute
-- 5 = Unknown
-- 6 = Voided trip

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type INT PRIMARY KEY,
    payment_desc TEXT NOT NULL
);

-- Insert payment types (use ON CONFLICT to avoid errors on re-run)
INSERT INTO dim_payment_type (payment_type, payment_desc) VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip')
ON CONFLICT (payment_type) DO NOTHING;

-- Vendor Dimension
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name TEXT NOT NULL
);

INSERT INTO dim_vendor (vendor_id, vendor_name) VALUES
(1, 'Creative Mobile Technologies'),
(2, 'VeriFone Inc.')
ON CONFLICT (vendor_id) DO NOTHING;

-- ======================================================
-- FACT TABLE
-- ======================================================

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id SERIAL PRIMARY KEY,

    -- Foreign keys to dimensions
    vendor_id INT,
    payment_type INT,

    -- Temporal fields
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,

    -- Trip metrics
    trip_distance FLOAT,
    total_amount FLOAT,

    -- Engineered/derived fields
    trip_hour INT,
    trip_weekday INT,
    trip_duration_min FLOAT,

    -- Constraints
    CONSTRAINT fk_payment_type FOREIGN KEY (payment_type) 
        REFERENCES dim_payment_type(payment_type),
    CONSTRAINT fk_vendor FOREIGN KEY (vendor_id) 
        REFERENCES dim_vendor(vendor_id)
);

-- ======================================================
-- INDEXES FOR QUERY PERFORMANCE
-- ======================================================

-- Time-series queries (e.g., trips over time)
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_datetime 
ON fact_trips (pickup_datetime);

-- Hourly analysis queries
CREATE INDEX IF NOT EXISTS idx_fact_trips_trip_hour 
ON fact_trips (trip_hour);

-- Day of week analysis queries
CREATE INDEX IF NOT EXISTS idx_fact_trips_trip_weekday 
ON fact_trips (trip_weekday);

-- Payment type distribution queries
CREATE INDEX IF NOT EXISTS idx_fact_trips_payment_type 
ON fact_trips (payment_type);

-- Vendor analysis queries
CREATE INDEX IF NOT EXISTS idx_fact_trips_vendor_id 
ON fact_trips (vendor_id);

-- ======================================================
-- ANALYTICAL VIEWS
-- Pre-aggregated views for common dashboard queries
-- These views reference public.fact_trips which Spark writes to
-- ======================================================

-- Trips per Hour of Day
CREATE OR REPLACE VIEW v_trips_by_hour AS
SELECT 
    trip_hour,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_fare
FROM public.fact_trips
GROUP BY trip_hour
ORDER BY trip_hour;

-- Trips per Day of Week
-- 1 = Sunday, 2 = Monday, ..., 7 = Saturday (PostgreSQL default)
CREATE OR REPLACE VIEW v_trips_by_weekday AS
SELECT 
    trip_weekday,
    CASE trip_weekday
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_name,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_fare
FROM public.fact_trips
GROUP BY trip_weekday
ORDER BY trip_weekday;

-- Payment Type Distribution
CREATE OR REPLACE VIEW v_payment_distribution AS
SELECT 
    f.payment_type,
    COALESCE(p.payment_desc, 'Unknown') AS payment_desc,
    COUNT(*) AS total_trips,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(SUM(f.total_amount)::numeric, 2) AS total_revenue
FROM public.fact_trips f
LEFT JOIN dim_payment_type p ON f.payment_type = p.payment_type
GROUP BY f.payment_type, p.payment_desc
ORDER BY total_trips DESC;

-- Average Trip Duration by Hour
CREATE OR REPLACE VIEW v_avg_duration_by_hour AS
SELECT
    trip_hour,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_duration_min)::numeric, 2) AS avg_duration_min,
    ROUND(MIN(trip_duration_min)::numeric, 2) AS min_duration_min,
    ROUND(MAX(trip_duration_min)::numeric, 2) AS max_duration_min
FROM public.fact_trips
WHERE trip_duration_min > 0 AND trip_duration_min < 180  -- Filter outliers
GROUP BY trip_hour
ORDER BY trip_hour;

-- Daily Trip Summary (for time-series dashboard)
CREATE OR REPLACE VIEW v_daily_summary AS
SELECT 
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
    ROUND(AVG(trip_duration_min)::numeric, 2) AS avg_duration_min
FROM public.fact_trips
GROUP BY DATE(pickup_datetime)
ORDER BY trip_date;

-- Vendor Performance Comparison
CREATE OR REPLACE VIEW v_vendor_performance AS
SELECT 
    f."VendorID" AS vendor_id,
    COALESCE(v.vendor_name, 'Unknown') AS vendor_name,
    COUNT(*) AS total_trips,
    ROUND(AVG(f.trip_distance)::numeric, 2) AS avg_distance,
    ROUND(AVG(f.total_amount)::numeric, 2) AS avg_fare,
    ROUND(AVG(f.trip_duration_min)::numeric, 2) AS avg_duration_min
FROM public.fact_trips f
LEFT JOIN dim_vendor v ON f."VendorID" = v.vendor_id
GROUP BY f."VendorID", v.vendor_name
ORDER BY total_trips DESC;

-- ======================================================
-- GRANT PERMISSIONS (for Metabase access)
-- ======================================================
GRANT USAGE ON SCHEMA nyc_taxi TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi TO PUBLIC;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA nyc_taxi TO PUBLIC;

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE '✅ NYC Taxi Data Warehouse initialized successfully!';
    RAISE NOTICE '📊 Schema: nyc_taxi';
    RAISE NOTICE '📋 Tables: dim_payment_type, dim_vendor, fact_trips';
    RAISE NOTICE '👁️ Views: v_trips_by_hour, v_trips_by_weekday, v_payment_distribution, v_avg_duration_by_hour, v_daily_summary, v_vendor_performance';
END $$;
