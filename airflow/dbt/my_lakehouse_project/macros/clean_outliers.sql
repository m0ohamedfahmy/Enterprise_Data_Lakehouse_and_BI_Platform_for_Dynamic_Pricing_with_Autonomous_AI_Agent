{% macro fill_outlier_with_avg(competitor_price_col, avg_price_col, upper_multiplier=5, lower_multiplier=0.1) %}
    CASE 
        WHEN {{ avg_price_col }} IS NULL 
            THEN ROUND(CAST({{ competitor_price_col }} AS DOUBLE), 2)

        WHEN CAST({{ competitor_price_col }} AS DOUBLE) > ({{ avg_price_col }} * {{ upper_multiplier }}) 
            THEN ROUND({{ avg_price_col }}, 2)

        WHEN CAST({{ competitor_price_col }} AS DOUBLE) < ({{ avg_price_col }} * {{ lower_multiplier }}) 
            THEN ROUND({{ avg_price_col }}, 2)

        ELSE ROUND(CAST({{ competitor_price_col }} AS DOUBLE), 2)
    END
{% endmacro %}

