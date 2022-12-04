
with subscription_active_dates as (
    select * from {{ ref('subscription_active_dates') }}
)

SELECT
      report_date as date
    , count(subscription_id) as subscriptions_active
FROM subscription_active_dates
GROUP BY 1
ORDER BY 1 ASC