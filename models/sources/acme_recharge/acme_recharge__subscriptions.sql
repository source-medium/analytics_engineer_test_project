with base as (
    select * from {{ ref('base__acme_recharge__subscriptions')}}
)

select *
from base
