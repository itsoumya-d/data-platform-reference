select
    account_id,
    customer_id,
    account_type,
    currency,
    bank,
    opened_date,
    status,
    balance_nzd
from {{ ref('stg_accounts') }}
