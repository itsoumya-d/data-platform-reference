select
    md5(account_id) as account_hk,
    updated_at as load_date,
    md5(cast(balance_nzd as varchar) || '|' || status) as hashdiff,
    balance_nzd,
    status,
    updated_at
from {{ ref('stg_accounts') }}
