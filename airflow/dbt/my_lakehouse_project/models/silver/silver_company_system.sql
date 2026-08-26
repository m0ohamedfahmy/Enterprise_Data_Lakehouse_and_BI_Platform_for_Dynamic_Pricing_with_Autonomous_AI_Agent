{{ config(
    materialized='incremental',
    unique_key=['transaction_id'],
    incremental_strategy='merge'
) }}
{%- set src_rel = source('bronze_data', 'raw_company_system') -%}
{%- set columns = adapter.get_columns_in_relation(src_rel) -%}
SELECT 
    {{ validate_allowed_values('"Record_Type"', ['SALES','STORE_EXPENSE']) }} AS record_type,
    {{ clean_with_pattern('"Transaction_ID"', '^Tx-[0-9]+$') }} AS transaction_id,
    {{ try_cast_date('"Date"', 'YYYY-MM-DD') }} AS transaction_date,
    {{ try_cast_int('"Store_ID"') }} AS store_id,
    {{ clean_with_pattern('"Store_Name"', '^Branch_[0-9]+$') }} AS store_name,
    {{ clean_title_case('"Store_City"') }} AS store_city,
    {{ map_store_region('"Store_City"', '"Store_Region"') }} AS store_region,
    {{ handle_not_applicable(try_cast_int('"Product_ID"'), "'Non-Product Expense'") }} AS product_id,
    {{ handle_not_applicable(clean_title_case('"Product_Name"'), "'Non-Product Expense'") }} AS product_name,
    {{ handle_not_applicable(clean_title_case('"Category"'), "'Non-Product Expense'") }} AS category,
    {{ handle_not_applicable(clean_title_case('"Sub_Category"'), "'Non-Product Expense'") }} AS sub_category,
    {{ try_cast_int('"Quantity"') }} AS quantity,
    {{ try_cast_double('"Unit_Cost"') }} AS unit_cost,
    {{ try_cast_double('"Unit_Retail"') }} AS unit_retail,
    {{ handle_not_applicable(clean_title_case('"Expense_Type"'), "'Direct Sale'") }} AS expense_type,
    {{ try_cast_double('"Expense_Amount"') }} AS expense_amount
FROM {{ src_rel }}
WHERE 
    1=1
    
    {%- for col in columns %}
         AND   {{ adapter.quote(col.column) }} IS NOT NULL
    {%- endfor %}

    {% if is_incremental() %}
    AND transaction_date > (SELECT COALESCE(MAX(transaction_date),'1900-01-01') FROM {{ this }})
    {% endif %}