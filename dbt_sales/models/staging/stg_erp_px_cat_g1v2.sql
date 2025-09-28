with raw as (
    select * 
    from {{ source('src_sales', 'ERP_PX_CAT_G1V2') }}
),
cleaned as (
    select
        id as product_id,
        cat as category,
        subcat as subcategory,
        maintenace as maintenance_flag
    from raw
),
derived as (
    select
        *,
        current_timestamp() as dwh_create_date
    from cleaned
)
select *
from derived