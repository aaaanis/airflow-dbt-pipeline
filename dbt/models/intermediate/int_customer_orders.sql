{{
  config(
    materialized = 'view'
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

customer_orders AS (
    SELECT
        o.order_id,
        c.customer_id,
        c.full_name AS customer_name,
        c.email AS customer_email,
        c.country AS customer_country,
        o.order_date,
        p.product_id,
        p.product_name,
        p.category AS product_category,
        o.quantity,
        o.price AS unit_price,
        o.total_amount,
        o.status AS order_status,
        -- Add audit columns
        CURRENT_TIMESTAMP AS updated_at
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products p ON o.product_id = p.product_id
)

SELECT * FROM customer_orders 