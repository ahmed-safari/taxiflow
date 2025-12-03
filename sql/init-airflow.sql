-- ======================================================
-- Create additional databases and users for the project
-- This runs BEFORE init.sql (hence the 00- prefix)
-- ======================================================

-- Create the demo user and sampledb database for the data warehouse
CREATE USER demo WITH PASSWORD 'demo';
CREATE DATABASE sampledb OWNER demo;
GRANT ALL PRIVILEGES ON DATABASE sampledb TO demo;

-- Grant demo user access to connect
ALTER USER demo CREATEDB;
