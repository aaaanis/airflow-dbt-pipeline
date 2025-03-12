{# Date formatting macros for consistent date transformations across models #}

{% macro format_date(date_expression, format='%Y-%m-%d') %}
    {# 
    Formats a date expression according to the specified format.
    
    Args:
        date_expression: The date column or expression to format
        format: The format string to use (default: %Y-%m-%d)
        
    Returns:
        A formatted date string
        
    Example:
        {{ format_date('order_date', '%d/%m/%Y') }} -> '31/12/2023'
    #}
    
    {% if target.type == 'postgres' %}
        to_char({{ date_expression }}::date, '{{ format }}')
    {% elif target.type == 'snowflake' %}
        to_varchar({{ date_expression }}::date, '{{ format }}')
    {% elif target.type == 'bigquery' %}
        format_date('{{ format }}', {{ date_expression }})
    {% else %}
        to_char({{ date_expression }}::date, '{{ format }}')
    {% endif %}
{% endmacro %}

{% macro date_part(date_expression, part) %}
    {# 
    Extracts a specific part from a date expression.
    
    Args:
        date_expression: The date column or expression
        part: The part to extract (year, quarter, month, day, dow, etc.)
        
    Returns:
        The specified part of the date
        
    Example:
        {{ date_part('created_at', 'year') }} -> 2023
    #}
    
    {% if target.type == 'postgres' %}
        extract({{ part }} from {{ date_expression }}::date)
    {% elif target.type == 'snowflake' %}
        date_part({{ part }}, {{ date_expression }}::date)
    {% elif target.type == 'bigquery' %}
        extract({{ part }} from {{ date_expression }})
    {% else %}
        extract({{ part }} from {{ date_expression }}::date)
    {% endif %}
{% endmacro %}

{% macro date_add(date_expression, interval_type, interval_value) %}
    {# 
    Adds a specified interval to a date expression.
    
    Args:
        date_expression: The date column or expression
        interval_type: The type of interval to add (day, month, year, etc.)
        interval_value: The amount to add (can be negative)
        
    Returns:
        The date with the interval added
        
    Example:
        {{ date_add('order_date', 'day', 7) }} -> order_date + 7 days
    #}
    
    {% if target.type == 'postgres' %}
        ({{ date_expression }}::date + interval '{{ interval_value }} {{ interval_type }}')::date
    {% elif target.type == 'snowflake' %}
        dateadd({{ interval_type }}, {{ interval_value }}, {{ date_expression }}::date)
    {% elif target.type == 'bigquery' %}
        date_add({{ date_expression }}, interval {{ interval_value }} {{ interval_type }})
    {% else %}
        ({{ date_expression }}::date + interval '{{ interval_value }} {{ interval_type }}')::date
    {% endif %}
{% endmacro %}

{% macro date_diff(end_date, start_date, diff_type='day') %}
    {# 
    Calculates the difference between two dates in the specified unit.
    
    Args:
        end_date: The end date expression
        start_date: The start date expression
        diff_type: The unit for the difference (day, month, year, etc.)
        
    Returns:
        The difference between the dates in the specified unit
        
    Example:
        {{ date_diff('end_date', 'start_date', 'month') }} -> Months between dates
    #}
    
    {% if target.type == 'postgres' %}
        {% if diff_type == 'day' %}
            ({{ end_date }}::date - {{ start_date }}::date)
        {% elif diff_type == 'month' %}
            (extract(year from {{ end_date }}::date) - extract(year from {{ start_date }}::date)) * 12 + 
            (extract(month from {{ end_date }}::date) - extract(month from {{ start_date }}::date))
        {% elif diff_type == 'year' %}
            (extract(year from {{ end_date }}::date) - extract(year from {{ start_date }}::date))
        {% else %}
            ({{ end_date }}::date - {{ start_date }}::date)
        {% endif %}
    {% elif target.type == 'snowflake' %}
        datediff({{ diff_type }}, {{ start_date }}::date, {{ end_date }}::date)
    {% elif target.type == 'bigquery' %}
        date_diff({{ end_date }}, {{ start_date }}, {{ diff_type }})
    {% else %}
        {% if diff_type == 'day' %}
            ({{ end_date }}::date - {{ start_date }}::date)
        {% elif diff_type == 'month' %}
            (extract(year from {{ end_date }}::date) - extract(year from {{ start_date }}::date)) * 12 + 
            (extract(month from {{ end_date }}::date) - extract(month from {{ start_date }}::date))
        {% elif diff_type == 'year' %}
            (extract(year from {{ end_date }}::date) - extract(year from {{ start_date }}::date))
        {% else %}
            ({{ end_date }}::date - {{ start_date }}::date)
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro is_weekend(date_expression) %}
    {# 
    Determines if a date falls on a weekend.
    
    Args:
        date_expression: The date column or expression
        
    Returns:
        Boolean indicating whether the date is a weekend (Saturday or Sunday)
        
    Example:
        {{ is_weekend('order_date') }} -> true/false
    #}
    
    {% if target.type == 'postgres' %}
        (extract(dow from {{ date_expression }}::date) = 0 or extract(dow from {{ date_expression }}::date) = 6)
    {% elif target.type == 'snowflake' %}
        (dayofweek({{ date_expression }}::date) = 0 or dayofweek({{ date_expression }}::date) = 6)
    {% elif target.type == 'bigquery' %}
        (extract(dayofweek from {{ date_expression }}) = 1 or extract(dayofweek from {{ date_expression }}) = 7)
    {% else %}
        (extract(dow from {{ date_expression }}::date) = 0 or extract(dow from {{ date_expression }}::date) = 6)
    {% endif %}
{% endmacro %}

{% macro fiscal_year(date_expression, start_month=4) %}
    {# 
    Calculates the fiscal year for a given date based on a specified starting month.
    
    Args:
        date_expression: The date column or expression
        start_month: The starting month of the fiscal year (default: 4 for April)
        
    Returns:
        The fiscal year for the given date
        
    Example:
        {{ fiscal_year('transaction_date', 4) }} -> 2023 (for dates from April 2023 to March 2024)
    #}
    
    {% set month_expression %}
        extract(month from {{ date_expression }}::date)
    {% endset %}
    
    {% set year_expression %}
        extract(year from {{ date_expression }}::date)
    {% endset %}
    
    case
        when {{ month_expression }} >= {{ start_month }} then {{ year_expression }}
        else {{ year_expression }} - 1
    end
{% endmacro %} 