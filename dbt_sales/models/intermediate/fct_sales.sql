{{ config(
    schema = 'GOLD',
) }}

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
        product_key,
        product_name,
        product_line,
        category_id
    from {{ ref('dim_product') }}
),

dim_cust as (
    select
        customer_id,
        customer_key,
        first_name,
        last_name,
        gender,
        marital_status,
        country,
        birth_date
    from {{ ref('dim_customer') }}
),

enriched as (
    select
        row_number() over (
            order by s.order_number, s.product_key
        ) as sales_sk,
        s.order_number,
        s.order_date,
        s.ship_date,
        s.due_date,
        s.sales_amount,
        s.quantity,
        s.unit_price,

        -- product dimension info
        p.product_key,
        p.product_name,
        p.product_line,
        p.category_id,

        -- customer dimension info
        c.customer_id,
        c.customer_key,
        c.first_name,
        c.last_name,
        c.gender,
        c.marital_status,
        c.country,
        c.birth_date,

        current_timestamp() as dwh_create_date
    from sales s
    left join dim_prod p on s.product_key = p.product_key
    left join dim_cust c on s.customer_id = c.customer_id
)

select *
from enriched
