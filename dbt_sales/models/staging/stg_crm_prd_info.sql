with raw as (
    select * 
    from {{ source('src_sales', 'CRM_PRD_INFO') }}
),
cleaned as (
    select
        prd_id::int as product_id,
        prd_key::string as full_product_key,
        prd_nm as product_name,
        COALESCE(prd_cost::decimal(10,2), 0) as product_cost,
        case UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS product_line,
        CAST(prd_start_dt AS DATE) as product_start_date,
        DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS product_end_date
    from raw
),
derived as (
    select
        *,
        REPLACE(SUBSTRING(full_product_key, 1, 5),'-','_') as category_id,
        SUBSTRING(full_product_key, 7, LEN(full_product_key)) as product_key,
        CURRENT_TIMESTAMP() as dwh_create_date
    from cleaned
)
select 
    product_id,
    category_id,
    product_key,
    product_name,
    product_cost,
    product_line,
    product_start_date,
    product_end_date,
    dwh_create_date
from derived