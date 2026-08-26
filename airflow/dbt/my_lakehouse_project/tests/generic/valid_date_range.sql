{% test valid_date_range(model, column_name, min_date='1900-01-01') %}

SELECT 
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (
      CAST({{ column_name }} AS DATE) < CAST('{{ min_date }}' AS DATE)
      OR CAST({{ column_name }} AS DATE) > CURRENT_DATE
  )

{% endtest %}