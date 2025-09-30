with sales as (
    select * from {{ ref('fct_sales') }}
),
products as (
    select 
        product_key,
        coalesce(product_cost, 0) as product_cost
    from {{ ref('dim_product') }}
)

select
    date_trunc('month', s.order_date) as month,
    sum(s.sales_amount) as total_revenue,
    sum(s.quantity * p.product_cost) as total_cost,
    sum(s.sales_amount) - sum(s.quantity * p.product_cost) as total_profit,
    case when sum(s.sales_amount) > 0 
         then round((sum(s.sales_amount) - sum(s.quantity * p.product_cost)) / sum(s.sales_amount) * 100, 2)
         else 0 end as profit_margin_pct
from sales s
join products p on s.product_key = p.product_key
group by 1
order by month
