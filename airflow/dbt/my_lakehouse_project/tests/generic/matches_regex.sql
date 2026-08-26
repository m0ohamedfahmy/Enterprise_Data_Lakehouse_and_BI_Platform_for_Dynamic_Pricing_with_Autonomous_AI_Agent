{% test matches_regex(model, column_name, regex) %}

SELECT 
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND NOT REGEXP_LIKE(CAST({{ column_name }} AS VARCHAR), '{{ regex }}')

{% endtest %}