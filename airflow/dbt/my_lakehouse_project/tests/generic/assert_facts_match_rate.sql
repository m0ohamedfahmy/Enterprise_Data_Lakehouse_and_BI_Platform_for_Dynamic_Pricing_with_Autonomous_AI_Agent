{% test assert_unmatched_keys_threshold(model, product_col='product_key', store_col='store_key', date_col='date_key', threshold=0.05) %}
/*
  Generic Test:
  Checks if the percentage of unmatched foreign keys (-1) in any specified column 
  exceeds the given threshold (default is 5%).
*/
WITH stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN {{ product_col }} = '-1' THEN 1 ELSE 0 END) AS unmatched_products,
        SUM(CASE WHEN {{ store_col }}   = '-1' THEN 1 ELSE 0 END) AS unmatched_stores,
        SUM(CASE WHEN {{ date_col }}    = '-1' THEN 1 ELSE 0 END) AS unmatched_dates
    FROM {{ model }}
)
SELECT *
FROM stats
WHERE unmatched_products > {{ threshold }} * total_rows
   OR unmatched_stores   > {{ threshold }} * total_rows
   OR unmatched_dates    > {{ threshold }} * total_rows

{% endtest %}