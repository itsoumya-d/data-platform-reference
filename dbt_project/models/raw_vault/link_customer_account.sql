select
    md5(customer_id || '|' || account_id) as customer_account_hk,
    md5(customer_id) as customer_hk,
    md5(account_id) as account_hk,
    min(opened_date) as load_date,
    'heartland_core' as record_source
from {{ ref('stg_accounts') }}
group by 1, 2, 3
