with raw as (
    select * 
    from {{ source('src_sales', 'CRM_CUST_INFO') }} -- raw source
),
ranked as (
    select 
        *,
        row_number() over (partition by cst_id order by cst_create_date desc) as flag_last -- rank records to get last
    from raw
    where cst_id is not null -- select the most recent record per customers
),
cleaned as (
    select
        cst_id::int as customer_id,
        cst_key::string as customer_key,
        TRIM(cst_firstname) as first_name,
        TRIM(cst_lastname) as last_name,
        case
            WHEN upper(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
            WHEN upper(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
            ELSE 'n/a'
        END AS marital_status,
        case
            WHEN upper(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
            WHEN upper(TRIM(cst_gndr)) = 'M' THEN 'MALE'
            ELSE 'n/a'
        END AS gender,
        cast(cst_create_date as date) as created_at
    from ranked
    where flag_last = 1 -- select the most recent record per customers
),
derived as (
    select
        *,
        current_timestamp() as dwh_create_date
    from cleaned
)
select *
from derived
