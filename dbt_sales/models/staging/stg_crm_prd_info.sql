with raw as (
    /*
    Step 1: Load raw CRM products data from the Bronze layer into the Silver (staging) layer
    */
    select * 
    from {{ source('src_sales', 'CRM_PRD_INFO') }}
),
cleaned as (
    /*
    Step 2: Data cleaning and standardization
    - Enforce correct data types
    - Normalize product line values
    - Handle null product costs
    - Derive product_end_date as the day before the next product_start_date
    */
    select
        prd_id::int as prd_id,
        prd_key::string as prd_key,
        prd_nm as prd_nm,
        COALESCE(prd_cost::decimal(10,2), 0) as prd_cost,
        case UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) as prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 DAY' AS DATE) AS prd_end_dt
    from raw
),
renamed as (
    /*
    Step 3: Apply business-friendly column names
    */
    select 
        prd_id as product_id,
        prd_key as full_product_key,
        prd_nm as product_name,
        prd_cost as product_cost,
        prd_line as product_line,
        prd_start_dt as product_start_date,
        prd_end_dt as product_end_date
    from cleaned
),
derived as (
    /*
    Step 4: Derive additional fields
    - Extract category_id from product key
    - Extract product_key from product key
    */
    select
        *,
        REPLACE(SUBSTRING(full_product_key, 1, 5),'-','_') as category_id,
        SUBSTRING(full_product_key, 7, LEN(full_product_key)) as product_key,
    from renamed
),
metadata as (
    /*
    Step 5: Add metadata for auditing and traceability
    */
    select
        *,
        CURRENT_TIMESTAMP() as dwh_create_date
    from derived
)
-- Final output: Cleaned, deduplicated, standardized customer dataset (Silver layer)
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
from metadata