{% macro validate_allowed_values(column_name, allowed_values) %}
    CASE 
        WHEN {{ column_name }} IS NULL OR TRIM({{ column_name }}) = '' THEN NULL

        WHEN UPPER(TRIM({{ column_name }})) IN (
            {% for val in allowed_values %}
                '{{ val | upper }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        THEN INITCAP(TRIM({{ column_name }}))

        ELSE NULL 
    END
{% endmacro %}