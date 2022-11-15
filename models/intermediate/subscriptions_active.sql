select
    date,
    count(*) as subscriptions_active

from {{ ref('subscriptions_days') }}
where is_subscription_active = true
group by 1
