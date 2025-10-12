with sales as (
    select 
        order_number,
        product_key,
        customer_id,
        order_date,
        ship_date,
        due_date,
        sales_amount,
        quantity,
        unit_price
    from {{ ref('stg_crm_sales_details') }}
),
dim_prod as (
    select
        product_key
        -- product_name,
        -- product_line,
        -- category_id
    from {{ ref('dim_product') }}
),
dim_cust as (
    select
        customer_id,
        customer_key
        -- first_name,
        -- last_name,
        -- gender,
        -- marital_status,
        -- country,
        -- birth_date
    from {{ ref('dim_customer') }}
),
enriched as (
    select
        s.order_number,
        s.product_key,
        c.customer_key,
        s.order_date,
        s.ship_date,
        s.due_date,
        s.sales_amount,
        s.quantity,
        s.unit_price,
        current_timestamp() as dwh_create_date
    from sales s
    left join dim_prod p on s.product_key = p.product_key
    left join dim_cust c on s.customer_id = c.customer_id
)
select *
from enriched