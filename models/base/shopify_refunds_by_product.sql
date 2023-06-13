{%- set schema_name,
        refund_table_name,
        adjustment_table_name,
        line_refund_table_name,
        transaction_table_name,
        order_line_table_name,
        product_table_name
        = 'shopify_raw',
        'refund',
        'order_adjustment',
        'order_line_refund',
        'transaction',
        'order_line',
        'product' -%}

{%- set refund_selected_fields = [
    "id",
    "order_id",
    "processed_at"
] -%}

{%- set adjustment_selected_fields = [
    "refund_id",
    "amount",
    "tax_amount",
    "kind"
] -%}

{%- set line_refund_selected_fields = [
    "refund_id",
    "quantity",
    "subtotal",
    "total_tax",
    "order_line_id"
] -%}

{%- set transaction_selected_fields = [
    "refund_id",
    "subtotal",
    "total_tax"
] -%}

{%- set order_line_selected_fields = [
    "id",
    "product_id"
] -%}

{%- set product_selected_fields = [
    "id",
    "title",
    "product_type"
] -%}

{%- set refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'refund') -%}
{%- set adjustment_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_adjustment') -%}
{%- set line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line_refund') -%}
{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line') -%}
{%- set product_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'product') -%}

WITH 
    -- To tackle the signal loss between Fivetran and Shopify transformations
    stellar_signal AS 
    (SELECT _fivetran_synced
    FROM {{ source('shopify_raw', 'order') }}
    LIMIT 1
    ),

    refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = refund_raw_tables) }}),

    refund_staging AS 
    (SELECT 
        
        {% for field in refund_selected_fields -%}
        {{ get_shopify_clean_field(refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM refund_raw_data
    ),

    adjustment_raw_data AS 
    ({{ dbt_utils.union_relations(relations = adjustment_raw_tables) }}),

    adjustment_staging AS 
    (SELECT 
        
        {% for field in adjustment_selected_fields -%}
        {{ get_shopify_clean_field(adjustment_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM adjustment_raw_data
    ),

    line_refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = line_refund_raw_tables) }}),

    line_refund_staging AS 
    (SELECT 
        
        {% for field in line_refund_selected_fields -%}
        {{ get_shopify_clean_field(line_refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM line_refund_raw_data
    ),

    order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),

    order_line_staging AS 
    (SELECT 
        
        {% for field in order_line_selected_fields -%}
        {{ get_shopify_clean_field(order_line_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_line_raw_data
    ),

    product_raw_data AS 
    ({{ dbt_utils.union_relations(relations = product_raw_tables) }}),

    product_staging AS 
    (SELECT 
        
        {% for field in product_selected_fields -%}
        {{ get_shopify_clean_field(product_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM product_raw_data
    ),

    order_line_product AS
    (SELECT
        order_line_id, 
        product_title,
        product_type
    FROM order_line_staging LEFT JOIN product_staging USING(product_id)
    ),

    line_refund AS 
    (SELECT 
        refund_id,
        product_title,
        product_type,
        COUNT(*) OVER (PARTITION BY refund_id) as product_count,
        COALESCE(SUM(refund_quantity),0) as quantity_refund, 
        COALESCE(SUM(refund_subtotal),0) as subtotal_refund,
        COALESCE(SUM(refund_total_tax),0) as total_tax_refund
    FROM line_refund_staging LEFT JOIN order_line_product USING(order_line_id)
    GROUP BY refund_id, product_title, product_type
    ),

    refund_adjustment AS
    (SELECT
        order_id,
        refund_id,
        processed_at as refund_date,
        CASE WHEN refund_kind ~* 'refund_discrepancy' THEN COALESCE(refund_amount,0) ELSE 0 END AS amount_discrepancy_refund,
        CASE WHEN refund_kind ~* 'refund_discrepancy' THEN COALESCE(refund_tax_amount,0) ELSE 0 END AS tax_amount_discrepancy_refund,
        CASE WHEN refund_kind ~* 'shipping_refund' THEN COALESCE(refund_amount,0) ELSE 0 END AS amount_shipping_refund,
        CASE WHEN refund_kind ~* 'shipping_refund' THEN COALESCE(refund_tax_amount,0) ELSE 0 END AS tax_amount_shipping_refund
        FROM refund_staging LEFT JOIN adjustment_staging USING(refund_id)
    ),

    refund_adjustment_line_refund AS 
    (SELECT 
        order_id,
        refund_id,
        product_title,
        product_type,
        refund_date,
        COALESCE(quantity_refund,0) AS quantity_refund,
        amount_discrepancy_refund::FLOAT/product_count::FLOAT AS amount_discrepancy_refund,
        tax_amount_discrepancy_refund::FLOAT/product_count::FLOAT AS tax_amount_discrepancy_refund,
        amount_shipping_refund::FLOAT/product_count::FLOAT AS amount_shipping_refund,
        tax_amount_shipping_refund::FLOAT/product_count::FLOAT AS tax_amount_shipping_refund,
        COALESCE(subtotal_refund,0) AS subtotal_refund,
        COALESCE(total_tax_refund,0) AS total_tax_refund
        FROM refund_adjustment
        LEFT JOIN line_refund USING(refund_id)
        --LEFT JOIN shopify_raw.order ON (order_id = id)
        --WHERE cancelled_at is null
    )

    SELECT 
        order_id, 
        refund_id,
        product_title,
        product_type,
        refund_date,
        quantity_refund,
        SUM(amount_discrepancy_refund) AS amount_discrepancy_refund,
        tax_amount_discrepancy_refund,
        SUM(amount_shipping_refund) AS amount_shipping_refund,
        SUM(tax_amount_shipping_refund) AS tax_amount_shipping_refund,
        subtotal_refund,
        total_tax_refund
    FROM refund_adjustment_line_refund
    GROUP BY order_id, refund_id, product_title, product_type, refund_date, quantity_refund, tax_amount_discrepancy_refund, subtotal_refund, total_tax_refund
