{{ config(
    tags = ["fact_store_expenses", "checks_grain"])
     }}
SELECT
    store_key,
    expense_type_key,
    month_date_key,
    COUNT(*) AS row_count
FROM {{ ref('fact_store_expenses') }}
GROUP BY store_key, expense_type_key, month_date_key
HAVING COUNT(*) > 1