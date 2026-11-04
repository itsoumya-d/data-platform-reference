with source as (
    select * from {{ raw_table('events') }}
)
select
    event_id,
    order_id,
    lower(trim(event_type)) as event_type,
    cast(event_ts as timestamp) as event_ts
from source
where event_id is not null
