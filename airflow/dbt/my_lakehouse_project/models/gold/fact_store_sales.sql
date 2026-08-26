{{ config(
    materialized='incremental',
    unique_key='sales_key',
    incremental_strategy='append'
) }}

WITH products_prepared AS (
    SELECT 
        p.*,
        MIN(p.dbt_valid_from) OVER(PARTITION BY p.product_key) AS min_product_valid_from
    FROM {{ ref('dim_products') }} p
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['s.transaction_id']) }} AS sales_key,
    s.transaction_id,
    COALESCE(p.product_key, -1) AS product_key,
    COALESCE(st.store_key, -1) AS store_key,
    COALESCE(d.date_key, -1) AS date_key,
    d.full_date,
    s.quantity,
    s.unit_cost,
    s.unit_retail,
    ROUND(s.quantity * s.unit_retail, 2) AS gross_revenue,
    ROUND(s.quantity * (s.unit_retail - s.unit_cost), 2) AS gross_margin_amount
FROM {{ ref('silver_sales') }} AS s
LEFT JOIN products_prepared AS p
    ON s.product_key = p.product_key
    AND(
        (s.transaction_date >= p.dbt_valid_from AND s.transaction_date < COALESCE(p.dbt_valid_to, CAST('9999-12-31' AS DATE)))
       OR
       (s.transaction_date < p.min_product_valid_from AND p.dbt_valid_from = p.min_product_valid_from) 
    )
LEFT JOIN {{ ref('dim_stores') }} AS st
    ON s.store_key = st.store_key
LEFT JOIN {{ ref('dim_date') }} AS d
    ON s.date_key = d.date_key    

{% if is_incremental() %}
  WHERE NOT EXISTS (
      SELECT 1 
      FROM {{ this }} f 
      WHERE f.transaction_id = s.transaction_id
  )
{% endif %}