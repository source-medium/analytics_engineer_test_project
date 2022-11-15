select
    date,
    count(*) as subscriptions_active

from {{ ref('subscriptions_days') }}
group by 1
