select
    md5(account_id) as account_hk,
    account_id,
    min(opened_date) as load_date,
    'heartland_core' as record_source
from {{ ref('stg_accounts') }}
group by 1, 2
