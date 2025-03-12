{{
  config(
    materialized = 'view'
  )
}}

WITH date_spine AS (
    -- Generate a date series from the earliest order to the latest order plus 30 days
    SELECT
        date_series::date AS date_day
    FROM
        generate_series(
            (SELECT MIN(order_date) FROM {{ ref('stg_orders') }}),
            (SELECT MAX(order_date) FROM {{ ref('stg_orders') }}) + INTERVAL '30 days',
            INTERVAL '1 day'
        ) AS date_series
),

date_dimensions AS (
    SELECT
        date_day,
        -- Use our custom date formatting macros
        {{ format_date('date_day', 'YYYY-MM-DD') }} AS formatted_date,
        {{ date_part('date_day', 'year') }} AS year,
        {{ date_part('date_day', 'quarter') }} AS quarter,
        {{ date_part('date_day', 'month') }} AS month,
        {{ date_part('date_day', 'day') }} AS day_of_month,
        {{ date_part('date_day', 'dow') }} AS day_of_week,
        -- Calculate if the date is a weekend
        {{ is_weekend('date_day') }} AS is_weekend,
        -- Fiscal year calculation (assuming fiscal year starts in April)
        {{ fiscal_year('date_day', 4) }} AS fiscal_year,
        -- Add 30 days to the date
        {{ date_add('date_day', 'day', 30) }} AS date_plus_30_days,
        -- Calculate the number of days since the start of the current year
        {{ date_diff('date_day', date_trunc('year', 'date_day')::date) }} AS days_since_year_start,
        -- Add an audit timestamp
        CURRENT_TIMESTAMP AS updated_at
    FROM date_spine
)

SELECT * FROM date_dimensions 