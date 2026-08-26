{{ config(
    tags = ["snapshot_products", "no_overlapping_scd2_windows"])
     }}
SELECT
    a.product_key,
    a.dbt_valid_from AS window_a_start,
    a.dbt_valid_to   AS window_a_end,
    b.dbt_valid_from AS window_b_start,
    b.dbt_valid_to   AS window_b_end
FROM {{ ref('snapshot_products') }} a
JOIN {{ ref('snapshot_products') }} b
    ON a.product_key = b.product_key
    AND a.dbt_valid_from < b.dbt_valid_from
    AND a.dbt_valid_from < COALESCE(b.dbt_valid_to, CAST('9999-12-31' AS DATE))
    AND COALESCE(a.dbt_valid_to, CAST('9999-12-31' AS DATE)) > b.dbt_valid_from