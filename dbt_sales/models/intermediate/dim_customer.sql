{{ config(
    schema = 'GOLD'
) }}

with crm_cust as (
    select 
        customer_id,
        customer_key,
        first_name,
        last_name,
        marital_status,
        gender,
        created_at
    from {{ ref('stg_crm_cust_info') }}
),
erp_cust as (
    select 
        customer_id,
        birth_date,
        gender
    from {{ ref('stg_erp_cust_az12') }}
),
erp_loc as (
    select 
        customer_id,
        country
    from {{ ref('stg_erp_loc_a101') }}
),
joined as (
    select
        c.customer_id,
        c.customer_key,
        c.first_name,
        c.last_name,
        l.country,
        c.marital_status,
        CASE 
            WHEN c.gender != 'n/a' THEN c.gender
            ELSE coalesce(e.gender, 'n/a')
        END AS gender,    -- prefer CRM, fallback to ERP
        e.birth_date,
        c.created_at
    from crm_cust c
    left join erp_cust e on c.customer_key = e.customer_id
    left join erp_loc l on c.customer_key = l.customer_id
),
final as (
    select
        row_number() over (order by customer_id) as customer_sk,  -- surrogate key
        *
    from joined
)
select * 
from final
