{{
  config(
    materialized = 'view'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'weather_data') }}
),

renamed AS (
    SELECT
        id,
        location,
        country,
        date,
        temperature,
        humidity,
        weather_condition,
        -- Add audit columns
        CURRENT_TIMESTAMP AS updated_at
    FROM source
)

SELECT * FROM renamed 