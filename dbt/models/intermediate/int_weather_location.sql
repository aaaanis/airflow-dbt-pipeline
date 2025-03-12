{{
  config(
    materialized = 'view'
  )
}}

WITH weather_data AS (
    SELECT * FROM {{ ref('stg_weather_data') }}
),

location_stats AS (
    SELECT
        location,
        country,
        AVG(temperature) AS avg_temperature,
        MIN(temperature) AS min_temperature,
        MAX(temperature) AS max_temperature,
        AVG(humidity) AS avg_humidity,
        -- Count the frequency of different weather conditions
        COUNT(*) AS total_measurements,
        -- Add audit columns
        CURRENT_TIMESTAMP AS updated_at
    FROM weather_data
    GROUP BY location, country
)

SELECT * FROM location_stats 