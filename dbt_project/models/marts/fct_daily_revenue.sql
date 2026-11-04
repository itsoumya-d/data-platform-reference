select
    cast(order_ts as date) as order_date,
    region,
    count(*) as orders,
    cast(sum(amount_nzd) as decimal(14, 2)) as revenue_nzd
from {{ ref('stg_orders') }}
where status = 'paid'
group by 1, 2
