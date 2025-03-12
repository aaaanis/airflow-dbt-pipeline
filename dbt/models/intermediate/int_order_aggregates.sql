{{
  config(
    materialized = 'view'
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_spent,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        -- Calculate recency, frequency, monetary value for RFM analysis
        CURRENT_DATE - MAX(order_date)::date AS days_since_last_order,
        -- Add audit columns
        CURRENT_TIMESTAMP AS updated_at
    FROM orders
    GROUP BY customer_id
)

SELECT * FROM customer_orders 