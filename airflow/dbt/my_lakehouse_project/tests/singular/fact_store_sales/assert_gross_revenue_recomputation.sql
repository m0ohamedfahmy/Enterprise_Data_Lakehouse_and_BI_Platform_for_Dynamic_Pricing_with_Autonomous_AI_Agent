{{ config(
    tags = ["fact_store_sales", "expected_gross_margin"])
     }}
WITH calculated_invariants AS (
    SELECT
        sales_key,
       ROUND(gross_revenue ,2) gross_revenue,
       ROUND(gross_margin_amount,2) gross_margin_amount,
        quantity,
        unit_cost,
        ROUND(gross_revenue - (quantity * unit_cost), 2) AS expected_gross_margin
    FROM {{ ref('fact_store_sales') }}
)

SELECT
    sales_key,
    gross_revenue,
    gross_margin_amount,
    expected_gross_margin
FROM calculated_invariants
WHERE gross_margin_amount != expected_gross_margin
