{{ config(
    materialized='incremental',
    unique_key=['competitor_price_key'],
    incremental_strategy='append'
) }}

WITH products_prepared AS (
    SELECT 
        p.*,
        MIN(p.dbt_valid_from) OVER(PARTITION BY p.product_key) AS min_product_valid_from
    FROM {{ ref('dim_products') }} p
)
SELECT
{{ dbt_utils.generate_surrogate_key(['sc.scraped_at','P.product_id','c.competitor_name','sc.competitor_price']) }} AS competitor_price_key,
COALESCE(sc.competitor_key, -1) AS competitor_key,
COALESCE(p.product_key, -1) AS product_key,
COALESCE(d.date_key, -1) AS date_key,
sc.scraped_at,
sc.competitor_price
FROM {{ ref('silver_competitor') }} AS sc  
LEFT JOIN products_prepared AS p
    ON sc.product_key = p.product_key
   AND (
       (sc.scraped_at >= p.dbt_valid_from AND sc.scraped_at < COALESCE(p.dbt_valid_to, CAST('9999-12-31' AS DATE)))
       OR
       (sc.scraped_at < p.min_product_valid_from AND p.dbt_valid_from = p.min_product_valid_from)
   )
LEFT JOIN {{ ref('dim_competitors') }} AS c
    ON sc.competitor_key = c.competitor_key
LEFT JOIN {{ ref('dim_date') }} AS d
    ON sc.date_key = d.date_key

{% if is_incremental() %}
  WHERE scraped_at > (SELECT MAX(scraped_at)  FROM {{ this }})
{% endif %}
