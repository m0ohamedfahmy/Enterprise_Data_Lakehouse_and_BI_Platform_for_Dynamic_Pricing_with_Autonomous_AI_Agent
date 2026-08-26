{{ config(
    materialized='incremental',
    unique_key='store_key',
    incremental_strategy='merge'
) }}

WITH aggregated_stores AS (
    SELECT
        store_key,
        store_id,
        store_name,
        store_city,
        store_region,
        MIN(transaction_date) AS create_at
    FROM {{ ref('silver_sales') }}
    GROUP BY 
        store_key,
        store_id,
        store_name,
        store_city,
        store_region
)

SELECT
    store_key,
    store_id,
    store_name,
    store_city,
    store_region,
    create_at
FROM aggregated_stores
WHERE 1=1
    {% if is_incremental() %}
    AND create_at > (
        SELECT COALESCE(MAX(create_at), CAST('1900-01-01' AS DATE)) 
        FROM {{ this }}
    )
    {% endif %}