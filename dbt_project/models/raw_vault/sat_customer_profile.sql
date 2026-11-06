select
    md5(customer_id) as customer_hk,
    cast(signup_date as timestamp) as load_date,
    md5(segment || '|' || country) as hashdiff,
    segment,
    country,
    signup_date
from {{ ref('stg_customers') }}
