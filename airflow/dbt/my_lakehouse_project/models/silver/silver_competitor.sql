{{ config(
    materialized='incremental',
    unique_key=['product_id', 'competitor_key', 'date_key'],
    incremental_strategy='merge'
) }}
{%- set src_rel_competitor = source('bronze_data', 'raw_competitor_scraper') -%}
{%- set columns = adapter.get_columns_in_relation(src_rel_competitor) -%}

WITH our_products AS (
    SELECT 
        product_id, 
        AVG(CAST(unit_retail AS DOUBLE)) AS our_unit_retail
    FROM {{ ref('silver_company_system') }}
    GROUP BY product_id
)  

SELECT
    {{ dbt_utils.generate_surrogate_key([clean_title_case('c."Competitor_Name"')]) }} AS competitor_key,
    {{ dbt_utils.generate_surrogate_key([try_cast_int('c."Product_ID"')]) }} AS product_key,
    CAST(TO_CHAR({{ try_cast_date('c."Scraped_At"', 'YYYY-MM-DD HH24:MI:SS') }}, 'YYYYMMDD') AS INT) AS date_key,
    {{ try_cast_date('c."Scraped_At"', 'YYYY-MM-DD HH24:MI:SS') }} AS scraped_at,
    {{ clean_title_case('c."Competitor_Name"') }} AS competitor_name,
    {{ try_cast_int('c."Product_ID"') }} AS product_id,
    {{ clean_with_pattern('c."Product_Name"', '^Prod_[0-9]+$') }} AS product_name,
    {{ fill_outlier_with_avg('c."Competitor_Price"', 'p."our_unit_retail"', upper_multiplier=5, lower_multiplier=0.1) }} AS competitor_price
FROM {{ src_rel_competitor }} AS c
LEFT JOIN our_products p 
    ON {{ try_cast_int('c."Product_ID"') }} = p.product_id 
WHERE 
    1=1
    {%- for col in columns %}
       AND c.{{ adapter.quote(col.column) }} IS NOT NULL
    {%- endfor %} 
     
    {% if is_incremental() %}
    AND {{ try_cast_date('c."Scraped_At"', 'YYYY-MM-DD HH24:MI:SS') }} > ( SELECT COALESCE(MAX(scraped_at), CAST('1900-01-01' AS TIMESTAMP)) FROM {{ this }} )
    {% endif %}