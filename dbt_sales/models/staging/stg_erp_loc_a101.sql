with raw as (
    select * 
    from {{ source('src_sales', 'ERP_LOC_A101') }}
),
cleaned as (
    select
        REPLACE(cid, '-', '') as customer_id,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States' 
            WHEN TRIM(cntry) = 'D' OR cntry IS NULL THEN 'n/a' 
            ELSE TRIM(cntry)
        END AS country, 
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