select
    cast(transaction_ts as date) as balance_date,
    account_id,
    count(*) as transaction_count,
    cast(sum(
        case when direction = 'credit' then amount_nzd else -amount_nzd end
    ) as decimal(18, 2)) as net_flow_nzd
from {{ ref('stg_transactions') }}
where status = 'posted'
group by 1, 2
