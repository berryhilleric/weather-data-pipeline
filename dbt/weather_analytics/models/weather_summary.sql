{{ config(materialized='table') }}

SELECT
    city,
    country,
    DATE(timestamp) as date,
    AVG(temperature) as avg_temperature,
    MAX(temperature) as max_temperature,
    MIN(temperature) as min_temperature,
    AVG(humidity) as avg_humidity,
    AVG(wind_speed) as avg_wind_speed,
    COUNT(*) as measurement_count
FROM raw.weather_data
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY city, country, DATE(timestamp)