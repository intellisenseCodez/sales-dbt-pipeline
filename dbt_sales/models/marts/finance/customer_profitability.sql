{{ config(materialized='table', schema='gold') }}

with sales as (
    select * from {{ ref('fct_sales') }}
),
customers as (
    select * from {{ ref('dim_customer') }}
),
products as (
    select 
        product_key,
        coalesce(product_cost, 0) as product_cost
    from {{ ref('dim_product') }}
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    sum(s.sales_amount) as total_revenue,
    sum(s.quantity * p.product_cost) as total_cost,
    sum(s.sales_amount) - sum(s.quantity * p.product_cost) as total_profit,
    case when sum(s.sales_amount) > 0 
         then round((sum(s.sales_amount) - sum(s.quantity * p.product_cost)) / sum(s.sales_amount) * 100, 2)
         else 0 end as profit_margin_pct
from sales s
join customers c on s.customer_id = c.customer_id
join products p on s.product_key = p.product_key
group by c.customer_id, c.first_name, c.last_name, c.country
order by total_profit desc
