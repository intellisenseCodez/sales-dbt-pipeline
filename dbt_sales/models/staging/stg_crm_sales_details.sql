with raw as (
    /*
    Step 1: Load raw CRM sales transactions from the Bronze layer to staging layer
    */
    select * 
    from {{ source('src_sales', 'CRM_SALES_DETAILS') }}
),
cleaned as (
    /*
    Step 2: Data cleaning and standardization
    - Convert product/customer IDs into the correct data types
    - Standardize dates: handle invalid formats (0 or wrong length) and convert to DATE
    - Fix data quality issues in sales and price calculations:
        * If sales amount is missing, non-positive, or inconsistent, recalculate as quantity * ABS(price)
        * If price is missing or invalid, derive it as sales / quantity
    */
    select
        sls_ord_num,
        sls_prd_key::string as sls_prd_key,
        sls_cust_id::int as sls_cust_id,
        -- Convert order_date from YYYYMMDD integer to DATE, handling invalid cases
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE to_date(sls_order_dt::string, 'YYYYMMDD')             
        END as sls_order_dt, 
        -- Convert order_date from YYYYMMDD integer to DATE, handling invalid cases
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE to_date(sls_ship_dt::string, 'YYYYMMDD')              
        END as sls_ship_dt, 
        -- Convert order_date from YYYYMMDD integer to DATE, handling invalid cases
        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE to_date(sls_due_dt::string, 'YYYYMMDD')              
        END as sls_due_dt, 
        -- Ensure sales_amount is consistent and valid
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales           
        END as sls_sales, 
        -- Enforce integer type for quantity
        sls_quantity::int as sls_quantity,
        -- Ensure price is valid; fallback calculation if missing/invalid
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / COALESCE(sls_quantity, NULL)
            ELSE sls_price           
        END as sls_price, 
    from raw
),
renamed as (
    /*
    Step 3: Apply business-friendly column names
    */
    select 
        sls_ord_num as order_number,
        sls_prd_key as product_key,
        sls_cust_id as customer_id,
        sls_order_dt as order_date,
        sls_ship_dt as ship_date,
        sls_due_dt as due_date,
        sls_sales as sales_amount,
        sls_quantity as quantity,
        sls_price as unit_price
    from cleaned
),
metadata as (
    /*
    Step 4: Add metadata for audit and traceability
    */
    select
        *,
        current_timestamp() as dwh_create_date
    from renamed
)
-- Final output: Cleaned, validated, and standardized sales transactions (Silver layer)
select *
from metadata