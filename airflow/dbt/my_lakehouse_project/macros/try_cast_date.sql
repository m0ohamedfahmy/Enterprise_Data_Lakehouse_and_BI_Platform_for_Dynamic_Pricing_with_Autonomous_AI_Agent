{% macro try_cast_date(column_name, date_format='YYYY-MM-DD') %}
    CASE 
        {% if date_format == 'YYYY-MM-DD' %}
            WHEN REGEXP_LIKE(TRIM({{ column_name }}), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') 
            THEN LEAST(CAST(TRIM({{ column_name }}) AS DATE), CURRENT_DATE())

        {% elif date_format in ['DD/MM/YYYY', 'MM/DD/YYYY'] %}
            WHEN REGEXP_LIKE(TRIM({{ column_name }}), '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') 
            THEN LEAST(TO_DATE(TRIM({{ column_name }}), '{{ date_format }}'), CURRENT_DATE())

        {% elif date_format == 'DD-MM-YYYY' %}
            WHEN REGEXP_LIKE(TRIM({{ column_name }}), '^[0-9]{2}-[0-9]{2}-[0-9]{4}$') 
            THEN LEAST(TO_DATE(TRIM({{ column_name }}), '{{ date_format }}'), CURRENT_DATE())

        {% elif date_format in ['YYYY-MM-DD HH24:MI:SS', 'YYYY-MM-DD HH24:MI:SS.FFF'] %}
            WHEN REGEXP_LIKE(TRIM({{ column_name }}), '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?$') 
            THEN LEAST(CAST(TRIM({{ column_name }}) AS DATE), CURRENT_DATE())

        {% else %}
            WHEN {{ column_name }} IS NOT NULL AND TRIM({{ column_name }}) != '' 
            THEN LEAST(TO_DATE(TRIM({{ column_name }}), '{{ date_format }}'), CURRENT_DATE())
        {% endif %}

        ELSE NULL 
    END
{% endmacro %}