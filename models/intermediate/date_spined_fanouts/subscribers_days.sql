-- first step for many subscriber metrics, get a model that is as the customer*day grain for
-- every day they had at least one active subscription.
select
    date,
    customer_id,
    count(subscription_id) as customer_subscriptions_count,
    max(cancelled_date) as cancelled_date

from {{ ref('subscriptions_days') }}
group by 1,2
