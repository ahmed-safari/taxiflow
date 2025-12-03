-- ======================================================
-- SCHEMA for NYC Taxi Data Warehouse
-- ======================================================

CREATE SCHEMA IF NOT EXISTS nyc_taxi;
SET search_path TO nyc_taxi;

-- ======================================================
-- DIMENSION TABLES
-- ======================================================

-- Payment Type Mapping
-- Reference:
-- 1 = Credit card
-- 2 = Cash
-- 3 = No charge
-- 4 = Dispute
-- 5 = Unknown
-- 6 = Voided trip

DROP TABLE IF EXISTS dim_payment_type CASCADE;
CREATE TABLE dim_payment_type (
    payment_type INT PRIMARY KEY,
    payment_desc TEXT
);

INSERT INTO dim_payment_type VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip');


-- ======================================================
-- FACT TABLE
-- ======================================================

DROP TABLE IF EXISTS fact_trips CASCADE;
CREATE TABLE fact_trips (
    trip_id SERIAL PRIMARY KEY,

    -- raw taxi data
    vendor_id INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_distance FLOAT,
    total_amount FLOAT,

    -- foreign key to dimension
    payment_type INT REFERENCES dim_payment_type(payment_type),

    -- engineered fields
    trip_hour INT,
    trip_weekday INT,
    trip_duration_min FLOAT
);

-- ======================================================
-- INDEXING FOR PERFORMANCE
-- ======================================================

-- Index for time-series queries
CREATE INDEX idx_fact_trips_pickup_datetime
ON fact_trips (pickup_datetime);

-- Index for hourly / weekday analytics
CREATE INDEX idx_fact_trips_trip_hour
ON fact_trips (trip_hour);

CREATE INDEX idx_fact_trips_trip_weekday
ON fact_trips (trip_weekday);

-- Index for payment type distribution
CREATE INDEX idx_fact_trips_payment_type
ON fact_trips (payment_type);
