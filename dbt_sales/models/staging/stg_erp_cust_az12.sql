with raw as (
    /*
    Step 1: Load raw ERP customer data from the Bronze layer
    */
    select * 
    from {{ source('src_sales', 'ERP_CUST_AZ12') }}
),
cleaned as (
    /*
    Step 2: Data cleaning and normalization
    - Standardize customer ID:
        * If the ID starts with 'NAS', remove the prefix and keep the remaining part
        * Otherwise, keep the original ID
    - Validate and clean birth_date:
        * Replace future dates with NULL since they are invalid
    - Normalize gender values:
        * Map 'F' or 'FEMALE' → 'Female'
        * Map 'M' or 'MALE' → 'Male'
        * Any other or missing value → 'n/a'
    */
    select
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid 
        END AS cid,
        CASE 
            WHEN bdate > CURRENT_DATE() THEN NULL 
            ELSE cast(bdate AS date) 
        END AS bdate, -- set future birthdate to null
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
            ELSE 'n/a'
        END AS gen,  -- normalize gender values and handle missing values
    from raw
),
renamed as (
    /*
    Step 3: Apply business-friendly column names
    */
    select 
        cid as customer_id,
        bdate as birth_date,
        gen as gender
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