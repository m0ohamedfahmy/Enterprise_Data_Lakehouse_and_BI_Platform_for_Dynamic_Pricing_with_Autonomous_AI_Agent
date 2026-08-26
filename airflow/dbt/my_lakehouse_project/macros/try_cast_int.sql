{% macro try_cast_int(column_name) %}
    CASE 
        WHEN REGEXP_LIKE(TRIM({{ column_name }}), '^-?[0-9]+(\.[0-9]+)?$') 
        THEN ABS(CAST(TRIM({{ column_name }}) AS INTEGER))
        ELSE NULL
    END
{% endmacro %}