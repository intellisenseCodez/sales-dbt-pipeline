with sales as (
    select * from {{ ref('fct_sales') }}
),
customers as (
    select * from {{ ref('dim_customer') }}
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    sum(s.sales_amount) as total_revenue,
    sum(s.quantity) as total_quantity
from sales s
join customers c on s.customer_id = c.customer_id
group by c.customer_id, c.first_name, c.last_name, c.country
order by total_revenue desc
