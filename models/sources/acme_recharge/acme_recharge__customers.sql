
with base_subscriptions as (
    select * from {{ ref('base__acme_recharge__subscriptions') }}
)

select
      customer_id
    , customer_email
    , EXTRACT(date from MIN(created_at)) as initial_subscription_for_customer_date
    , EXTRACT(date from MAX(created_at)) as most_recent_subscription_for_customer_date
    , MAX(cancelled_date) as most_recent_cancellation_for_customer_date
    , MAX(cancelled_effective_date) as most_recent_cancellation_for_customer_effective_date
from base_subscriptions
group by 1,2
