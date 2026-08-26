{{ config(
    materialized='incremental',
    unique_key='competitor_key',
    incremental_strategy='merge'
) }}

WITH aggregated_competitors AS (
    SELECT
        competitor_key,
        competitor_name,
        MIN(scraped_at) AS create_at  
    FROM {{ ref('silver_competitor') }}
    GROUP BY
        competitor_key,
        competitor_name
)

SELECT
    competitor_key,
    competitor_name,
    create_at
FROM aggregated_competitors
WHERE 1=1
    {% if is_incremental() %}
    AND create_at > (
        SELECT COALESCE(MAX(create_at), CAST('1900-01-01' AS TIMESTAMP)) 
        FROM {{ this }}
    )
    {% endif %}