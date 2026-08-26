{{ config(
    tags = ["fact_store_sales", "unmatched_products"])
     }}
WITH stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN product_key = '-1' THEN 1 ELSE 0 END) AS unmatched_products,
        SUM(CASE WHEN store_key = '-1' THEN 1 ELSE 0 END) AS unmatched_stores,
        SUM(CASE WHEN date_key = '-1' THEN 1 ELSE 0 END) AS unmatched_dates
    FROM {{ ref('fact_store_sales') }}
)
SELECT *
FROM stats
WHERE unmatched_products > 0.05 * total_rows
   OR unmatched_stores   > 0.05 * total_rows
   OR unmatched_dates    > 0.05 * total_rows