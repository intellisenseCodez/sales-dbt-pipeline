with sales as (
    select * from {{ ref('fct_sales') }}
)

select
    order_date,
    sum(quantity) as total_quantity,
    sum(sales_amount) as total_revenue
from sales
group by order_date
order by order_date
