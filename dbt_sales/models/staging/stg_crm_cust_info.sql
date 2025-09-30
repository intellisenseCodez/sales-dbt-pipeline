with raw as (
    /*
    Step 1: Load raw CRM customer data from the Bronze layer into the Silver (staging) layer
    */
    select * 
    from {{ source('src_sales', 'CRM_CUST_INFO') }}
),
ranked as (
    /*
    Step 2: Deduplicate records by ranking rows per customer.
    - Historical data contains multiple records for the same customer.
    - Use row_number() to keep only the most recent record based on cst_create_date.
    */
    select 
        *,
        row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
    from raw
),
cleaned as (
    /*
    Step 3: Data cleaning and normalization.
    - Cast fields to correct data types.
    - Standardize gender and marital status values.
    - Trim whitespace from string fields.
    - Replace missing values with (n/a).
    - Filter to only the latest (flag_last = 1) and non-null customer IDs.
    */
    select
        cst_id::int as cst_id,
        cst_key::string as cst_key,
        TRIM(cst_firstname) as cst_first_name,
        TRIM(cst_lastname) as cst_last_name,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
            ELSE 'n/a'
        END AS cst_gndr,
        cast(cst_create_date as date) as cst_create_date
    from ranked
    where flag_last = 1 and cst_id is not null
),
renamed as (
    /*
    Step 4: Apply business-friendly column names for downstream use.
    */
    select 
        cst_id as customer_id,
        cst_key as customer_key,
        cst_first_name as first_name,
        cst_last_name as last_name,
        cst_marital_status as marital_status,
        cst_gndr as gender,
        cst_create_date as create_at
    from cleaned
),
metadata as (
    /*
    Step 5: Add metadata fields for audit and traceability.
    */
    select
        *,
        CURRENT_TIMESTAMP() as dwh_create_date
    from renamed
)
-- Final output: Cleaned, deduplicated, standardized customer dataset (Silver layer)
select *
from metadata
