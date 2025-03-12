{{
  config(
    materialized = 'view'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

renamed AS (
    SELECT
        product_id,
        product_name,
        category,
        description,
        price,
        created_at,
        -- Add audit columns
        CURRENT_TIMESTAMP AS updated_at
    FROM source
)

SELECT * FROM renamed 