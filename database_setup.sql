-- Create database (run this as postgres superuser)
CREATE DATABASE weather_db;

-- Connect to the new database before running the rest of the commands
-- \c weather_db

-- Create schema
CREATE SCHEMA IF NOT EXISTS weather_db_schema;

-- Create historical weather data table
CREATE TABLE IF NOT EXISTS weather_db_schema.historical_weather_data (
    id SERIAL PRIMARY KEY,
    observation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature NUMERIC(5,2) NOT NULL,
    condition VARCHAR(100) NOT NULL,
    fetch_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- Create view for daily temperature trends
CREATE OR REPLACE VIEW weather_db_schema.daily_temperature_view AS
SELECT 
    fetch_date AS date,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp,
    AVG(temperature) AS avg_temp,
    COUNT(*) AS observation_count
FROM 
    weather_db_schema.historical_weather_data
GROUP BY 
    fetch_date
ORDER BY 
    fetch_date DESC;


-- Create user role for Airflow with appropriate permissions
CREATE ROLE airflow_weather_role;
GRANT USAGE ON SCHEMA weather_db_schema TO airflow_weather_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA weather_db_schema TO airflow_weather_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA weather_db_schema TO airflow_weather_role;

-- Create database user for Airflow to connect with (if needed)
-- CREATE USER airflow_weather_user WITH PASSWORD 'secure_password_here';
-- GRANT airflow_weather_role TO airflow_weather_user;

-- Grant permissions on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA weather_db_schema
GRANT SELECT, INSERT, UPDATE ON TABLES TO airflow_weather_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA weather_db_schema
GRANT USAGE, SELECT ON SEQUENCES TO airflow_weather_role;