{{ config(
    materialized='table',
    unique_key='month_key'
) }}

WITH date_range AS (
    SELECT
        {% if is_incremental() %}
            (SELECT DATE_ADD(MAX(month_start_date), CAST(1 AS INTERVAL MONTH)) FROM {{ this }}) AS start_date,
        {% else %}
            DATE_TRUNC('year', MIN(CAST(scraped_at AS DATE))) AS start_date,
        {% endif %}
        
        DATE_ADD(DATE_TRUNC('year', MAX(CAST(scraped_at AS DATE))), CAST(1 AS INTERVAL YEAR)) AS end_date
    FROM {{ ref('silver_competitor') }}
),

numbers AS (
    {{ dbt_utils.generate_series(1200) }}
),

all_months AS (
    SELECT
        DATE_ADD(dr.start_date, CAST(CAST(n.generated_number - 1 AS INT) AS INTERVAL MONTH)) AS date_month
    FROM numbers n
    CROSS JOIN date_range dr
)

SELECT
    CAST(TO_CHAR(date_month, 'YYYYMM') AS INT) AS month_key,
    CAST(date_month AS DATE) AS month_start_date,
    CAST(LAST_DAY(date_month) AS DATE) AS month_end_date,
    TO_CHAR(date_month, 'Month') AS month_name,
    CAST(EXTRACT(QUARTER FROM date_month) AS INT) AS "quarter",
    CAST(EXTRACT(YEAR FROM date_month) AS INT) AS "year"

FROM all_months
WHERE date_month >= (SELECT start_date FROM date_range)
  AND date_month <  (SELECT end_date FROM date_range)