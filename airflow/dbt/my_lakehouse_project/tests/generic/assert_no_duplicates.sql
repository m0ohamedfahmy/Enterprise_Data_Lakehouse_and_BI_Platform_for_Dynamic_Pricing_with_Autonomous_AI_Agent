{% test assert_no_duplicates(model, column_name) %}
    {{ config(severity = 'warn') }}
    SELECT 
        {{ column_name }} AS duplicate_column,
        COUNT(*) AS occurrence_count
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 1

{% endtest %}

