with source as (
    select * from {{ raw_table('orders') }}
),
deduped as (
    select
        *,
        row_number() over (partition by order_id order by order_ts desc) as rn
    from source
)
select
    order_id,
    customer_id,
    cast(order_ts as timestamp) as order_ts,
    lower(trim(region)) as region,
    cast(amount_nzd as decimal(12, 2)) as amount_nzd,
    lower(trim(status)) as status
from deduped
where rn = 1
  and order_id is not null
