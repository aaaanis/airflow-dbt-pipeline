{{
  config(
    materialized = 'table'
  )
}}

WITH weather_locations AS (
    SELECT * FROM {{ ref('int_weather_location') }}
),

final AS (
    SELECT
        -- Create a surrogate key for the dimension
        {{ dbt_utils.generate_surrogate_key(['location', 'country']) }} AS location_key,
        location,
        country,
        avg_temperature,
        min_temperature,
        max_temperature,
        avg_humidity,
        total_measurements,
        -- Classification of locations based on average temperature
        CASE 
            WHEN avg_temperature > 25 THEN 'Hot'
            WHEN avg_temperature > 15 THEN 'Warm'
            WHEN avg_temperature > 5 THEN 'Cool'
            ELSE 'Cold'
        END AS temperature_category,
        -- Classification of locations based on humidity
        CASE 
            WHEN avg_humidity > 75 THEN 'High Humidity'
            WHEN avg_humidity > 50 THEN 'Medium Humidity'
            ELSE 'Low Humidity'
        END AS humidity_category,
        -- Add audit columns
        updated_at
    FROM weather_locations
)

SELECT * FROM final 