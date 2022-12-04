
with subscription_active_dates as (
    select * from {{ ref('subscription_active_dates') }}
),

customer_dates as (
    select * from {{ ref('customer_dates') }}
)

select
      sad.date
    , count(sad.subscription_id) as subscriptions_active
    , sum(cd.subscriptions_active_count) as subscriptions_active_v2

    , sum(sad.is_new_subscription_int) as subscriptions_new
    , sum(sad.is_cancelled_subscription_int) as subscriptions_cancelled
    , sum(cd.is_new_customer_int) as subscribers_new
    , sum(cd.is_cancelled_customer_int) as subscribers_cancelled
    , sum(cd.is_active_customer_int) as subscribers_active
from subscription_active_dates as sad
left join customer_dates as cd
    on sad.date = cd.date
    and sad.customer_id = cd.customer_id
GROUP BY 1
ORDER BY 1 ASC