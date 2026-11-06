select
    case when account_type = 'loan' then 'lending' else 'deposits' end as book,
    count(*) as accounts,
    cast(sum(balance_nzd) as decimal(18, 2)) as balance_nzd
from {{ ref('dim_account') }}
where status = 'active'
group by 1
