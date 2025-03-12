{{
  config(
    materialized = 'view'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        email,
        gender,
        country,
        created_at,
        -- Add audit columns
        CURRENT_TIMESTAMP AS updated_at
    FROM source
)

SELECT * FROM renamed 