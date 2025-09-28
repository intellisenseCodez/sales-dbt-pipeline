with raw as (
    select * 
    from {{ source('src_sales', 'ERP_CUST_AZ12') }}
),
cleaned as (
    select
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid 
        END AS customer_id,
        CASE 
            WHEN bdate > CURRENT_DATE() THEN NULL 
            ELSE cast(bdate AS date) 
        END AS birth_date, -- set future birthdate to null
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
            ELSE 'n/a'
        END AS gender,  -- normalize gender values and handle missing values
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