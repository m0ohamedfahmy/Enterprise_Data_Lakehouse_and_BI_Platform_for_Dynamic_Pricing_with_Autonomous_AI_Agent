{% test no_scd2_overlapping_windows(
    model, 
    id_col='product_key', 
    valid_from_col='dbt_valid_from', 
    valid_to_col='dbt_valid_to'
) %}

SELECT
    a.{{ id_col }},
    a.{{ valid_from_col }} AS window_a_start,
    a.{{ valid_to_col }}   AS window_a_end,
    b.{{ valid_from_col }} AS window_b_start,
    b.{{ valid_to_col }}   AS window_b_end
FROM {{ model }} a
INNER JOIN {{ model }} b
    ON a.{{ id_col }} = b.{{ id_col }}
    AND a.{{ valid_from_col }} < b.{{ valid_from_col }}
    AND a.{{ valid_from_col }} < COALESCE(b.{{ valid_to_col }}, CAST('9999-12-31' AS DATE))
    AND COALESCE(a.{{ valid_to_col }}, CAST('9999-12-31' AS DATE)) > b.{{ valid_from_col }}

{% endtest %}