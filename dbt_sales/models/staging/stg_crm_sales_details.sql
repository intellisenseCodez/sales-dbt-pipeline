with raw as (
    select * 
    from {{ source('src_sales', 'CRM_SALES_DETAILS') }}
),
cleaned as (
    select
        sls_ord_num as order_number,
        sls_prd_key as product_key,
        sls_cust_id::int as customer_id,
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE to_date(sls_order_dt::string, 'YYYYMMDD')              -- convert int to date type
        END as order_date, 
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE to_date(sls_ship_dt::string, 'YYYYMMDD')              -- convert int to date type
        END as ship_date, 
        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE to_date(sls_due_dt::string, 'YYYYMMDD')              -- convert int to date type
        END as due_date, 
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales           
        END as sales_amount, 
        sls_quantity::int as quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / COALESCE(sls_quantity, NULL)
            ELSE sls_price           
        END as unit_price, 
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