{% macro handle_not_applicable(column_expression, replacement_value="'Non-Product Expense'") %}
    CASE 
        WHEN {{ column_expression }} IS NULL 
          OR TRIM(CAST({{ column_expression }} AS VARCHAR)) IN ('NOT_APPLICABLE', 'N/A', 'NONE', '', 'Not_Applicable') 
        THEN {{ replacement_value }}
        
        ELSE {{ column_expression }}
    END
{% endmacro %}