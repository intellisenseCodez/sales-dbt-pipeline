with sales as (
    select * from {{ ref('fct_sales') }}
),
customers as (
    select
        customer_id,
        customer_key,
        first_name,
        last_name,
        country,
        marital_status,
        gender
    from {{ ref('dim_customer') }}
)

select
    c.customer_key,
    c.first_name,
    c.last_name,
    c.country,
    c.gender,
    count(distinct s.order_number) as total_orders,
    sum(s.sales_amount) as total_revenue,
    sum(s.quantity) as total_units_purchased,
    avg(s.sales_amount) as avg_order_value,
    min(s.order_date) as first_purchase_date,
    max(s.order_date) as last_purchase_date,
    datediff(day, min(s.order_date), max(s.order_date)) as customer_span_days
from sales s
join customers c on s.customer_key = c.customer_key
group by 
    c.customer_key, c.first_name, c.last_name, 
    c.country, c.gender
order by total_revenue desc
