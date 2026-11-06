with source as (
    select * from {{ raw_table('accounts') }}
)
select
    account_id,
    customer_id,
    lower(trim(account_type)) as account_type,
    upper(trim(currency)) as currency,
    upper(trim(bank)) as bank,
    cast(opened_date as date) as opened_date,
    lower(trim(status)) as status,
    cast(balance_nzd as decimal(18, 2)) as balance_nzd,
    cast(updated_at as timestamp) as updated_at
from source
where account_id is not null
