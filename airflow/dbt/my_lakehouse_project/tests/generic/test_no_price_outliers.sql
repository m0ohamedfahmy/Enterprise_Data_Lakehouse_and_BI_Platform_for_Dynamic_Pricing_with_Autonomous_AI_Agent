{% test test_cleaned_price_bounds(model, column_name, ref_model, group_by_col, price_ref_col, upper_multiplier=5, lower_multiplier=0.1) %}

WITH ref_data AS (
    SELECT 
        {{ group_by_col }} AS ref_id,
        AVG(CAST({{ price_ref_col }} AS DOUBLE)) AS avg_ref_price
    FROM {{ ref_model }}
    GROUP BY {{ group_by_col }}
)

SELECT 
    m.{{ column_name }},
    r.avg_ref_price
FROM {{ model }} m
INNER JOIN ref_data r 
    ON m.product_id = r.ref_id
WHERE r.avg_ref_price IS NOT NULL
  AND m.{{ column_name }} IS NOT NULL
  AND (
      m.{{ column_name }} > (r.avg_ref_price * {{ upper_multiplier }})
      OR m.{{ column_name }} < (r.avg_ref_price * {{ lower_multiplier }})
  )

{% endtest %}