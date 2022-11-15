select
    cancelled_date as date,
    count(*) as subscriptions_cancelled

from {{ ref('base__recharge_subscriptions') }}
where was_cancelled = true
group by 1
