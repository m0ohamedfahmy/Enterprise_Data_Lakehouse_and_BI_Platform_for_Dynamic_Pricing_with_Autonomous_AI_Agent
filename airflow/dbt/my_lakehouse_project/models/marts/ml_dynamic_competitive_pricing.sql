{{ config(
    materialized='view'
) }}

WITH weekly_sales AS (
    SELECT
        s.product_key,
        s.store_key,
        DATE_TRUNC('week', d.full_date) AS sales_week,
        AVG(s.unit_retail) AS avg_unit_retail,
        AVG(s.unit_cost) AS avg_unit_cost,
        SUM(s.quantity) AS total_quantity_sold,
        COUNT(DISTINCT s.transaction_id) AS total_transactions
    FROM {{ ref('fact_store_sales') }} s
    JOIN {{ ref('dim_date') }} d ON s.date_key = d.date_key
    WHERE s.product_key != '-1' AND s.store_key != '-1' AND s.date_key != '-1'
    GROUP BY 1, 2, 3
),

weekly_spine AS (
    SELECT DISTINCT
        DATE_TRUNC('week', full_date) AS sales_week
    FROM {{ ref('dim_date') }}
    WHERE full_date BETWEEN (SELECT MIN(sales_week) FROM weekly_sales)
                        AND (SELECT MAX(sales_week) FROM weekly_sales)
),

active_products AS (
    SELECT 
        product_key,
        product_id,
        product_name,
        category,
        sub_category
    FROM {{ ref('dim_products') }}
    WHERE dbt_valid_to IS NULL AND product_key != '-1'
),

active_stores AS (
    -- dim_stores is Type 1 (no SCD tracking), so no valid_to filter needed
    -- here - every row is already "current" by definition.
    SELECT
        store_key,
        store_id,
        store_name,
        store_region,
        store_city
    FROM {{ ref('dim_stores') }}
    WHERE store_key != '-1'
),

product_time_grid AS (
    -- Grid is now product x store x week - every combination gets a row,
    -- same forward-fill logic as before but per-store now, not blended
    -- across stores.
    SELECT 
        p.product_key,
        p.product_id,
        p.product_name,
        p.category,
        p.sub_category,
        st.store_key,
        st.store_id,
        st.store_name,
        st.store_region,
        st.store_city,
        w.sales_week
    FROM active_products p
    CROSS JOIN active_stores st
    CROSS JOIN weekly_spine w
),

scraped_competitor_prices AS (
    -- Deliberately still product x week only, no store_key here.
    -- fact_competitor_prices has no store dimension at all - competitor
    -- prices are a market-level signal, not scoped to any one of your
    -- stores. Do not join this on store_key; every store for a given
    -- product should see the same competitor price for a given week.
    SELECT 
        c.product_key,
        DATE_TRUNC('week', d.full_date) AS sales_week, 
        AVG(c.competitor_price) AS avg_competitor_price,
        MIN(c.competitor_price) AS min_competitor_price,  
        MAX(c.competitor_price) AS max_competitor_price   
    FROM {{ ref('fact_competitor_prices') }} c
    JOIN {{ ref('dim_date') }} d ON c.date_key = d.date_key
    WHERE c.product_key != '-1'
    GROUP BY 1, 2
),

value_flagged AS (
    SELECT
        grid.product_key,
        grid.store_key,
        grid.sales_week,
        
        ws.avg_unit_retail,
        ws.avg_unit_cost,
        ws.total_quantity_sold,
        ws.total_transactions,
        
        cp.avg_competitor_price,
        cp.min_competitor_price,
        cp.max_competitor_price,
        
        CASE WHEN ws.avg_unit_retail IS NOT NULL THEN 1 ELSE 0 END AS has_sales_value,
        CASE WHEN cp.avg_competitor_price IS NOT NULL THEN 1 ELSE 0 END AS has_comp_value
        
    FROM product_time_grid grid
    LEFT JOIN weekly_sales ws
        ON grid.product_key = ws.product_key
       AND grid.store_key = ws.store_key
       AND grid.sales_week = ws.sales_week
    LEFT JOIN scraped_competitor_prices cp
        ON grid.product_key = cp.product_key
       AND grid.sales_week = cp.sales_week
        -- no store_key in this join condition - see note above
),

value_grouped AS (
    SELECT
        *,
        -- Sales carry-forward is now per (product, store) - a store with
        -- a real sales gap forward-fills its OWN last known price, not
        -- another store's.
        SUM(has_sales_value) OVER (
            PARTITION BY product_key, store_key ORDER BY sales_week
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sales_carry_group,
        
        -- Competitor carry-forward stays per product only - correct,
        -- since the underlying data has no store dimension to partition by.
        SUM(has_comp_value) OVER (
            PARTITION BY product_key ORDER BY sales_week
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS comp_carry_group
    FROM value_flagged
),

competitor_filled AS (
    SELECT
        product_key,
        store_key,
        sales_week,
        
        total_quantity_sold,
        total_transactions,
        
        MAX(avg_unit_retail) OVER (PARTITION BY product_key, store_key, sales_carry_group) AS filled_unit_retail,
        MAX(avg_unit_cost) OVER (PARTITION BY product_key, store_key, sales_carry_group) AS filled_unit_cost,
        
        -- comp_carry_group was computed per product only, but this window
        -- still needs store_key here so the fill applies to every store's
        -- row independently rather than collapsing them together.
        MAX(avg_competitor_price) OVER (PARTITION BY product_key, store_key, comp_carry_group) AS filled_comp_avg,
        MAX(min_competitor_price) OVER (PARTITION BY product_key, store_key, comp_carry_group) AS filled_comp_min,
        MAX(max_competitor_price) OVER (PARTITION BY product_key, store_key, comp_carry_group) AS filled_comp_max
        
    FROM value_grouped
)

SELECT
    grid.product_id, 
    grid.sales_week,
    grid.product_name,
    grid.category,
    grid.sub_category,

    grid.store_key,
    grid.store_id,
    grid.store_name,
    grid.store_region,
    grid.store_city,
    
    ROUND(CAST(COALESCE(cf.filled_unit_retail, 0) AS NUMERIC), 2) AS avg_unit_retail,
    ROUND(CAST(COALESCE(cf.filled_unit_cost, 0) AS NUMERIC), 2) AS avg_unit_cost,
    
    ROUND(CAST(COALESCE(cf.filled_comp_avg, cf.filled_unit_retail, 0) AS NUMERIC), 2) AS avg_competitor_price,
    ROUND(CAST(COALESCE(cf.filled_comp_min, cf.filled_unit_retail, 0) AS NUMERIC), 2) AS min_competitor_price,
    ROUND(CAST(COALESCE(cf.filled_comp_max, cf.filled_unit_retail, 0) AS NUMERIC), 2) AS max_competitor_price,
    
    COALESCE(cf.total_quantity_sold, 0) AS quantity_sold,
    COALESCE(cf.total_transactions, 0) AS total_transactions

FROM product_time_grid grid
JOIN competitor_filled cf 
    ON grid.product_key = cf.product_key 
   AND grid.store_key = cf.store_key
   AND grid.sales_week = cf.sales_week