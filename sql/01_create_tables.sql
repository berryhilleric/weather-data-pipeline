-- Raw data tables
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(10),
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    humidity INTEGER,
    pressure INTEGER,
    wind_speed DECIMAL(5,2),
    weather_description VARCHAR(200),
    timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.air_quality_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(10),
    location_name VARCHAR(200),
    parameter VARCHAR(50),
    value DECIMAL(10,2),
    unit VARCHAR(20),
    timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp ON raw.weather_data(city, timestamp);
CREATE INDEX IF NOT EXISTS idx_airquality_city_timestamp ON raw.air_quality_data(city, timestamp);