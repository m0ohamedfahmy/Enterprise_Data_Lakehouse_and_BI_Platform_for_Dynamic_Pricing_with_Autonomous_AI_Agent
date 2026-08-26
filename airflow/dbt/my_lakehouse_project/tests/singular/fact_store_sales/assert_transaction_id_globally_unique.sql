{{ config(
    tags = ["fact_store_sales", "no_duplicate_transaction_id"])
     }}
SELECT
    transaction_id,
    COUNT(*) AS row_count
FROM {{ ref('silver_sales') }}
GROUP BY transaction_id
HAVING COUNT(*) > 1