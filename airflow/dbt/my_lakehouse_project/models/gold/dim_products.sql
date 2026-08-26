{{ config(
    materialized='view',
) }}

WITH snapshot_data AS (
    SELECT
    *
    FROM {{ ref('snapshot_products') }} 
),

unknown_product AS (
   
    SELECT
        -1                                    AS product_key,
        -1                                    AS product_id,
        'Unknown / Missing Product'          AS product_name,
        'Undefined'                           AS category,
        'Undefined'                           AS sub_category,
        CAST('1970-01-01' AS DATE)            AS updated_at,
        'placeholder_unknown_product_key_-1' AS dbt_scd_id,
        CAST('1970-01-01' AS DATE)            AS dbt_updated_at,
        CAST('1970-01-01' AS DATE)            AS dbt_valid_from,
        CAST(NULL AS DATE)                    AS dbt_valid_to
)

SELECT * FROM unknown_product

UNION ALL

SELECT * FROM snapshot_data