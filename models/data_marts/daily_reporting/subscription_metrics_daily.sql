
with subscription_active_dates as (
    select * from {{ ref('subscription_active_dates') }}
),

customer_active_dates as (
    select * from {{ ref('customer_active_dates') }}
),

metric_aggregates as (
    select
        cd.date
        , sum(cd.total_new_subscriptions) as subscriptions_new
        , sum(cd.returning_customer_subscription_count) as subscriptions_returning
        , sum(cd.total_cancelled_subscriptions) as subscriptions_cancelled
        , sum(cd.subscriptions_active_count) as subscriptions_active
        , sum(cd.is_new_customer_int) as subscribers_new
        , sum(cd.is_cancelled_customer_int) as subscribers_cancelled
        , sum(cd.is_cancelled_effetive_date_customer_int) as subscribers_cancelled_next_day_effective
        , sum(cd.is_active_customer_int) as subscribers_active
    from customer_active_dates as cd
    where cd.date <= PARSE_DATE('%m/%d/%Y', '04/07/2022') -- this would be set to current date with fresher date
    group by 1
    order by 1 asc
)

select 
     *
    , sum(ma.subscriptions_cancelled) over (order by date asc) as subscriptions_churned
    -- this isnt perfect, a customer who returns later and cancels again is counted twice,
    -- a customer who left but came back still counts as churned
    , sum(ma.subscribers_cancelled_next_day_effective) over (order by date asc) as subscribers_churned 
from metric_aggregates as ma
