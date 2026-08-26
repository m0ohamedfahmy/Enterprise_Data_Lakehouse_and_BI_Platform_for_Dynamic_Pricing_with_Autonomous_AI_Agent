{% macro dropna(model_ref) %}
    {%- set columns = adapter.get_columns_in_relation(model_ref) -%}
    
    SELECT *
    FROM {{ model_ref }}
    WHERE 
    {%- for col in columns %}
        {{ col.column }} IS NOT NULL
        {%- if not loop.last %} AND {% endif %}
    {%- endfor %}

{% endmacro %}