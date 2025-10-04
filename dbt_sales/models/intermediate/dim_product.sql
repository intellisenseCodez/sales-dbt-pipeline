with crm_prd as (
    select
        product_id,
        category_id,
        product_key,
        product_name,
        product_cost,
        product_line,
        product_start_date,
        product_end_date
    from {{ ref('stg_crm_prd_info') }}
    where product_end_date is not null -- filter out all historical data
),
erp_cat as (
    select
        product_id,
        category,
        subcategory,
        maintenance_flag
    from {{ ref('stg_erp_px_cat_g1v2') }}
),
joined as (
    select
        p.product_key,
        p.category_id,
        p.product_name,
        p.product_line,
        p.product_cost,
        c.category,
        c.subcategory,
        c.maintenance_flag,
        p.product_start_date,
        p.product_end_date
    from crm_prd p
    left join erp_cat c on p.category_id = c.product_id
),
final as (
    select
        row_number() over (order by product_key) as product_sk, -- surrogate key
        *
    from joined
)
select *
from final
