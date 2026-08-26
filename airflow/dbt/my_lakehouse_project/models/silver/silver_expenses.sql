{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}
SELECT 
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} AS expense_key,
    CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT) AS month_key,
    {{ dbt_utils.generate_surrogate_key(['CAST(store_id AS INT)']) }} AS store_key,
    {{ dbt_utils.generate_surrogate_key(['expense_type']) }} AS expense_type_key,
    transaction_id,
    transaction_date,
    store_id,
    store_name,
    store_city,
    store_region,
    expense_type,
    expense_amount
FROM {{ ref('silver_company_system') }} 
WHERE record_type = 'Store_Expense'

{% if is_incremental() %}
    AND transaction_date > (SELECT COALESCE(MAX(transaction_date),'1900-01-01') FROM {{ this }})
{% endif %}