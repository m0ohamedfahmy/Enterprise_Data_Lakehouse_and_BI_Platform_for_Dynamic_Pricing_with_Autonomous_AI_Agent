{{ config(
    tags = ["fact_store_expenses", "checks_join"])
     }}

WITH expenses_aggregated AS (
    SELECT
        store_key,
        expense_type_key,
        month_key,
        SUM(COALESCE(expense_amount, 0)) AS expense_amount
    FROM {{ ref('silver_expenses') }}
    GROUP BY store_key, expense_type_key,month_key
     ),
    

source_count AS (
    SELECT COUNT(*) AS cnt FROM expenses_aggregated
),
fact_count AS (
    SELECT COUNT(*) AS cnt FROM {{ ref('fact_store_expenses') }}
)
SELECT s.cnt AS source_rows, f.cnt AS fact_rows
FROM source_count s
CROSS JOIN fact_count f
WHERE s.cnt != f.cnt