with raw as (
    /*
    Step 1: Load raw ERP Location data from the Bronze layer
    */
    select * 
    from {{ source('src_sales', 'ERP_LOC_A101') }}
),
cleaned as (
    /*
    Step 2: Data cleaning and standardization
    - Replace values
    - Handle trailing white spaces
    - Normalize country values
    - Handle null product costs
    */
    select
        REPLACE(cid, '-', '') as cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States' 
            WHEN TRIM(cntry) = 'D' OR cntry IS NULL THEN 'n/a' 
            ELSE TRIM(cntry)
        END AS cntry, 
    from raw
),
renamed as (
    /*
    Step 3: Apply business-friendly column names
    */
    select
        cid as customer_id,
        cntry as country
    from cleaned
),
metadata as (
    /*
    Step 4: Add metadata for auditing and traceability
    */
    select
        *,
        current_timestamp() as dwh_create_date
    from renamed
)
select *
from metadata