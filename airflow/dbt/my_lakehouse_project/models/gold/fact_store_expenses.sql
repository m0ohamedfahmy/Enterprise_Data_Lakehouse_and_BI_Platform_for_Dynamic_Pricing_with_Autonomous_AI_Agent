{{ config(
    materialized='incremental',
    unique_key='expense_key',
    incremental_strategy='append'
) }}

WITH expenses_aggregated AS (

    SELECT
        store_key,
        expense_type_key,
        month_key,
        SUM(COALESCE(expense_amount, 0)) AS expense_amount
    FROM {{ ref('silver_expenses') }}
    GROUP BY store_key, expense_type_key, month_key 
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['st.store_key', 'et.expense_type_key', 'd.month_key']) }} AS expense_key,
    COALESCE(d.month_key, -1) AS month_date_key,
    COALESCE(st.store_key, -1) AS store_key,
    COALESCE(et.expense_type_key, -1) AS expense_type_key,
    d.month_start_date,
    se.expense_amount
FROM expenses_aggregated se
LEFT JOIN {{ ref('dim_stores') }} AS st
    ON se.store_key = st.store_key
LEFT JOIN {{ ref('dim_expense_type') }} AS et
    ON se.expense_type_key = et.expense_type_key    
LEFT JOIN {{ ref('dim_month') }} AS d
    ON se.month_key = d.month_key
{% if is_incremental() %}
  WHERE NOT EXISTS (
      SELECT 1
      FROM {{ this }} f
      WHERE f.expense_key = expense_key
  )
{% endif %}
