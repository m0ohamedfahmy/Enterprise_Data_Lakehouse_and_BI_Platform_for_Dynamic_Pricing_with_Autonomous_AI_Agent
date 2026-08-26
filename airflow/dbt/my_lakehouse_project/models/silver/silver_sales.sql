{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}
WITH clean_master_mapping AS (
    SELECT * FROM {{ ref('product_mapping') }}
)
SELECT 
    p.record_type,
    p.transaction_id,
    p.transaction_date,
    p.store_id,
    p.store_name,
    p.store_city,
    p.store_region,
    p.product_id,
    COALESCE(m.product_name, p.product_name) AS product_name,
    COALESCE(m.category, p.category) AS category,
    COALESCE(m.sub_category, p.sub_category) AS sub_category,
    p.quantity,
    p.unit_cost,
    p.unit_retail,
    p.expense_type,
    p.expense_amount,
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['p.store_id']) }} AS store_key,
    CAST(TO_CHAR(p.transaction_date, 'YYYYMMDD') AS INT) AS date_key
FROM {{ ref('silver_company_system') }} AS p
left join clean_master_mapping m 
    on p.product_id = m.product_id
WHERE p.record_type = 'Sales' AND p.quantity > 0

{% if is_incremental() %}
   AND (
      p.transaction_date > (SELECT COALESCE(MAX(transaction_date), CAST('1900-01-01' AS DATE)) FROM {{ this }})
  )
{% endif %}