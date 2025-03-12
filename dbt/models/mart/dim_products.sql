{{
  config(
    materialized = 'table'
  )
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_orders AS (
    SELECT 
        product_id,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(quantity) AS total_quantity_sold,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('int_customer_orders') }}
    GROUP BY product_id
),

final AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.description,
        p.price,
        p.created_at,
        -- Add sales metrics
        COALESCE(po.order_count, 0) AS order_count,
        COALESCE(po.total_quantity_sold, 0) AS total_quantity_sold,
        COALESCE(po.total_revenue, 0) AS total_revenue,
        -- Calculate product popularity metrics
        CASE 
            WHEN COALESCE(po.order_count, 0) > 5 THEN 'High'
            WHEN COALESCE(po.order_count, 0) > 2 THEN 'Medium'
            ELSE 'Low'
        END AS popularity,
        -- Add audit columns
        p.updated_at
    FROM products p
    LEFT JOIN product_orders po ON p.product_id = po.product_id
)

SELECT * FROM final 