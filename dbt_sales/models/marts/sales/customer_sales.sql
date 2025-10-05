with sales as (
    select * from {{ ref('fct_sales') }}
),
customers as (
    select * from {{ ref('dim_customer') }}
)
select
    c.customer_key,
    c.first_name,
    c.last_name,
    c.country,
    sum(s.sales_amount) as total_revenue,
    sum(s.quantity) as total_quantity
from sales s
join customers c on s.customer_key = c.customer_key
group by c.customer_key, c.first_name, c.last_name, c.country
order by total_revenue desc
