{% snapshot snapshot_products %}

    {{
        config(
          target_schema='gold',
          strategy='timestamp',
          unique_key='product_key',
          updated_at='updated_at',
        )
    }}

    SELECT
        product_key,
        product_id,
        product_name,
        category,
        sub_category,
        MAX(transaction_date) AS updated_at
    FROM {{ ref('silver_sales') }}
    GROUP BY
        product_key,
        product_id,
        product_name,
        category,
        sub_category

{% endsnapshot %}