{{
  config(
    materialized = 'view'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        product_id,
        quantity,
        price,
        total_amount,
        status,
        -- Add audit columns
        CURRENT_TIMESTAMP AS updated_at
    FROM source
)

SELECT * FROM renamed 