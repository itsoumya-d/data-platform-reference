select
    md5(customer_id) as customer_hk,
    customer_id,
    min(signup_date) as load_date,
    'heartland_core' as record_source
from {{ ref('stg_customers') }}
group by 1, 2
