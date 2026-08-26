{{ config(
    materialized='table',
    unique_key='date_key',
    incremental_strategy='merge'
) }}

WITH date_range AS (
    SELECT
        {% if is_incremental() %}
            (SELECT DATE_ADD(MAX(full_date), CAST(1 AS INTERVAL DAY)) FROM {{ this }}) AS start_date,
        {% else %}
            DATE_TRUNC('year', MIN(CAST(transaction_date AS DATE))) AS start_date,
        {% endif %}
        DATE_ADD(DATE_TRUNC('year', MAX(CAST(transaction_date AS DATE))), CAST(1 AS INTERVAL YEAR)) AS end_date
    FROM {{ ref('silver_sales') }}
),


numbers AS (
    {{ dbt_utils.generate_series(3650) }}
),

all_dates AS (
    SELECT
        DATE_ADD(dr.start_date, CAST(CAST(n.generated_number - 1 AS INT) AS INTERVAL DAY)) AS full_date
    FROM numbers n
    CROSS JOIN date_range dr
)

SELECT
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INT) AS date_key,
    CAST(full_date AS DATE) AS full_date,
    CAST(EXTRACT(DAY FROM full_date) AS INT) AS "day",
    CAST(EXTRACT(MONTH FROM full_date) AS INT) AS "month",
    CAST(EXTRACT(QUARTER FROM full_date) AS INT) AS "quarter",
    CAST(EXTRACT(YEAR FROM full_date) AS INT) AS "year",
    CASE 
        WHEN EXTRACT(DOW FROM full_date) IN (5, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM all_dates
WHERE full_date >= (SELECT start_date FROM date_range)
  AND full_date <  (SELECT end_date FROM date_range)