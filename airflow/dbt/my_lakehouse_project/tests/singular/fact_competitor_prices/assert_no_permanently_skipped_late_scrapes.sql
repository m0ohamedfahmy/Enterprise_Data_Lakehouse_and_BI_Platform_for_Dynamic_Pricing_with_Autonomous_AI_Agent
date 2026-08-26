{{ config(
    tags = ["fact_competitor_prices", "Late_Arriving_data"])
     }}
WITH max_loaded AS (
    SELECT MAX(scraped_at) AS max_scraped_at
    FROM {{ ref('fact_competitor_prices') }}
),
source_before_max AS (
    SELECT DISTINCT scraped_at
    FROM {{ ref('silver_competitor') }}, max_loaded
    WHERE scraped_at <= max_loaded.max_scraped_at
),
target_before_max AS (
    SELECT DISTINCT scraped_at
    FROM {{ ref('fact_competitor_prices') }}, max_loaded
    WHERE scraped_at <= max_loaded.max_scraped_at
)
SELECT s.scraped_at
FROM source_before_max s
LEFT JOIN target_before_max t ON s.scraped_at = t.scraped_at
WHERE t.scraped_at IS NULL