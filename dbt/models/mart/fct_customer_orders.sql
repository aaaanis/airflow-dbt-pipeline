{{
  config(
    materialized = 'table'
  )
}}

WITH customer_orders AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
),

final AS (
    SELECT
        -- Surrogate key for the fact table
        {{ dbt_utils.generate_surrogate_key(['order_id', 'customer_id', 'product_id']) }} AS order_key,
        order_id,
        customer_id,
        customer_name,
        customer_email,
        customer_country,
        order_date,
        product_id,
        product_name,
        product_category,
        quantity,
        unit_price,
        total_amount,
        order_status,
        -- Extract date parts for time-based analysis
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        EXTRACT(DAY FROM order_date) AS order_day,
        EXTRACT(DOW FROM order_date) AS order_day_of_week,
        -- Add audit columns
        updated_at
    FROM customer_orders
)

SELECT * FROM final 