
with subscription_active_dates as (
    select * from {{ ref('subscription_active_dates') }}
)

SELECT
      date
    , count(subscription_id) as subscriptions_active
    , sum(is_new_subscription_int) as subscriptions_new
    , sum(is_cancelled_subscription_int) as subscriptions_cancelled
FROM subscription_active_dates
GROUP BY 1
ORDER BY 1 ASC