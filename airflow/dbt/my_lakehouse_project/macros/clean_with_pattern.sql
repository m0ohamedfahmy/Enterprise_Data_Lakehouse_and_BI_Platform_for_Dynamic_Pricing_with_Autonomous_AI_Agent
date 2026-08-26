{% macro clean_with_pattern(column_name, pattern='^Branch_[a-zA-Z0-9]+$') %}
    CASE 
        WHEN {{ column_name }} IS NULL OR TRIM({{ column_name }}) = '' THEN NULL
        
        WHEN REGEXP_LIKE(TRIM({{ column_name }}), '{{ pattern }}') 
        THEN INITCAP(TRIM({{ column_name }}))
        
        ELSE NULL 
    END
{% endmacro %}