select
    created_date as date,
    count(*) as subscriptions_new

from {{ ref('base__recharge_subscriptions') }}
group by 1
