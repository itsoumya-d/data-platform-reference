with source as (
    select * from {{ raw_table('transactions') }}
)
select
    transaction_id,
    account_id,
    cast(transaction_ts as timestamp) as transaction_ts,
    cast(amount_nzd as decimal(18, 2)) as amount_nzd,
    lower(trim(direction)) as direction,
    lower(trim(category)) as category,
    lower(trim(status)) as status
from source
where transaction_id is not null
