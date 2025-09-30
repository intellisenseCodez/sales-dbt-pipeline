with sales as (
    select * from {{ ref('fct_sales') }}
),
products as (
    select * from {{ ref('dim_product') }}
)

select
    p.product_key,
    p.product_name,
    p.category,
    sum(s.quantity) as total_quantity,
    sum(s.sales_amount) as total_revenue
from sales s
join products p on s.product_key = p.product_key
group by p.product_key, p.product_name, p.category
order by total_revenue desc
