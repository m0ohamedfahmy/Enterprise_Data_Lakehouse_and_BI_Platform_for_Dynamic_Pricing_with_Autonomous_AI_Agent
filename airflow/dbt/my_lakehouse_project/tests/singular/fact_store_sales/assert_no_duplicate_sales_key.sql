{{ config(
    tags = ["fact_store_sales", "no_duplicate_sales_key"])
     }}
SELECT
    sales_key,
    COUNT(*) AS row_count
FROM {{ ref('fact_store_sales') }}
GROUP BY sales_key
HAVING COUNT(*) > 1