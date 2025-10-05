with sales as (
    select * from {{ ref('fct_sales') }}
),
products as (
    select * from {{ ref('dim_product') }}
),
customers as (
    select * from {{ ref('dim_customer') }}
)

select
    date_trunc(month, s.order_date) as order_month,
    date_trunc(quarter, s.order_date) as order_quarter,
    date_trunc(year, s.order_date) as order_year,
    sum(s.sales_amount) as total_revenue,
    sum(s.quantity) as total_quantity,
    count(distinct s.order_number) as total_orders,
    count(distinct s.customer_key) as unique_customers,
    count(distinct s.product_key) as unique_products
from sales s
left join customers c on s.customer_key = c.customer_id
left join products p on s.product_key = p.product_key
group by 1, 2, 3
order by order_month
