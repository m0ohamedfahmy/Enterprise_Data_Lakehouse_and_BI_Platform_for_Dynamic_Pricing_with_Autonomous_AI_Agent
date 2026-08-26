{{ config(
    materialized='incremental',
    unique_key='expense_type_key',
    incremental_strategy='merge'
) }}

WITH aggregated_expense_types AS (
    SELECT
        expense_type_key,
        expense_type,
        MIN(transaction_date) AS create_at  
    FROM {{ ref('silver_expenses') }}
    GROUP BY
        expense_type_key,
        expense_type
)

SELECT
    expense_type_key,
    expense_type,
    create_at
FROM aggregated_expense_types
WHERE 1=1
    {% if is_incremental() %}
    AND create_at > (
        SELECT COALESCE(MAX(create_at), CAST('1900-01-01' AS DATE)) 
        FROM {{ this }}
    )
    {% endif %}