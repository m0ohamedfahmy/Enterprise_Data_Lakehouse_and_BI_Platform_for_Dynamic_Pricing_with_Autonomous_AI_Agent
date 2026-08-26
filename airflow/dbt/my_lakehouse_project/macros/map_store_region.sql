{% macro map_store_region(city_column, region_column) %}
    CASE 
        
        WHEN UPPER(TRIM({{ city_column }})) IN ('CAIRO', 'GIZA') 
            THEN 'Greater Cairo'

        WHEN UPPER(TRIM({{ city_column }})) = 'ALEXANDRIA' 
            THEN 'Alex Coast'

        WHEN UPPER(TRIM({{ city_column }})) IN ('TANTA', 'MANSOURA') 
            THEN 'Delta'

        WHEN {{ region_column }} IS NOT NULL AND TRIM({{ region_column }}) != '' 
            THEN INITCAP(TRIM({{ region_column }}))

        ELSE NULL 
    END
{% endmacro %}