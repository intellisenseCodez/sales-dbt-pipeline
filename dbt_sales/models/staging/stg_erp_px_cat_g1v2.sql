with raw as (
    /*
    Step 1: Load raw ERP Products data from the Bronze layer
    */
    select * 
    from {{ source('src_sales', 'ERP_PX_CAT_G1V2') }}
),
renamed as (
    /*
    Step 2: Apply business-friendly column names
    */
    select
        id as product_id,
        cat as category,
        subcat as subcategory,
        maintenace as maintenance_flag
    from raw
),
metadata as (
    /*
    Step 3: Add metadata for auditing and traceability
    */
    select
        *,
        current_timestamp() as dwh_create_date
    from renamed
)
select *
from metadata