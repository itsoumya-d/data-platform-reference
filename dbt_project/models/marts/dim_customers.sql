with source as (
    select * from {{ raw_table('customers') }}
)
select
    customer_id,
    lower(trim(segment)) as segment,
    upper(trim(country)) as country,
    cast(signup_date as date) as signup_date
from source
where customer_id is not null
