{{ config(materialized='table') }}

SELECT
    city,
    country,
    parameter,
    DATE(timestamp) as date,
    AVG(value) as avg_value,
    MAX(value) as max_value,
    MIN(value) as min_value,
    STDDEV(value) as stddev_value,
    COUNT(*) as measurement_count,
    COUNT(DISTINCT location_name) as location_count
FROM raw.air_quality_data
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY city, country, parameter, DATE(timestamp)
