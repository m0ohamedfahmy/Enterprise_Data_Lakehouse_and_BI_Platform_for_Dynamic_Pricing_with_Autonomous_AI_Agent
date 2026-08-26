{% macro clean_title_case(column_name) %}
    CASE 
        WHEN {{ column_name }} IS NULL OR TRIM({{ column_name }}) = '' THEN NULL
        ELSE INITCAP(TRIM({{ column_name }}))
    END
{% endmacro %}