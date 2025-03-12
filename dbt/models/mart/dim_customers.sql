{{
  config(
    materialized = 'table'
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_orders AS (
    SELECT * FROM {{ ref('int_order_aggregates') }}
),

final AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.gender,
        c.country,
        c.created_at,
        -- Add order statistics
        COALESCE(co.total_orders, 0) AS total_orders,
        COALESCE(co.total_spent, 0) AS lifetime_value,
        co.first_order_date,
        co.last_order_date,
        co.days_since_last_order,
        -- Customer segmentation based on RFM (Recency, Frequency, Monetary)
        CASE 
            WHEN co.days_since_last_order <= 30 AND co.total_orders >= 3 AND co.total_spent >= 500 THEN 'High Value'
            WHEN co.days_since_last_order <= 90 AND co.total_orders >= 2 THEN 'Regular'
            WHEN co.days_since_last_order > 90 THEN 'Churned'
            WHEN co.total_orders = 1 THEN 'New'
            ELSE 'Low Activity'
        END AS customer_segment,
        -- Add audit columns
        GREATEST(c.updated_at, COALESCE(co.updated_at, c.updated_at)) AS updated_at
    FROM customers c
    LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
)

SELECT * FROM final 