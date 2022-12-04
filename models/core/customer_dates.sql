
with subscription_active_dates as (
    select * from {{ ref('subscription_active_dates') }}
),

customers as (
    select * from {{ ref('acme_recharge__customers') }}
),

date_spine as (
    select * from {{ ref('utilities__dates') }}
),

subscription_active_dates_agg_customer as (
    SELECT
          customer_id
        , date
        , count(*) as subscriptions_active_count
    from subscription_active_dates
    group by 1, 2
),

customer_dates as (

    select
          ds.date_day as date
        , c.*
    from customers as c
    left join date_spine as ds
        on ds.date_day >= c.initial_subscription_for_customer_date
        and ds.date_day <= c.most_recent_subscription_for_customer_date
)

select
      cd.*
    , ifnull(sad.subscriptions_active_count, 0) as subscriptions_active_count
    , if(cd.initial_subscription_for_customer_date = cd.date, 1, 0) as is_new_customer_int
    , if(sad.subscriptions_active_count > 0, 1, 0) as is_active_customer_int
    , if(
        LAG(sad.subscriptions_active_count) over (partition by cd.customer_id order by cd.date)
            > 0
        and sad.subscriptions_active_count is null
    , 1, 0) as is_cancelled_customer_int
    , if(
        LAG(sad.subscriptions_active_count) over (partition by cd.customer_id order by cd.date)
            is null
        and sad.subscriptions_active_count > 0
        and cd.initial_subscription_for_customer_date != cd.date 
    , 1, 0) as is_returning_customer_int
    , if(
        LAG(sad.subscriptions_active_count) over (partition by cd.customer_id order by cd.date)
            is null
        and sad.subscriptions_active_count > 0
        and cd.initial_subscription_for_customer_date != cd.date 
    , sad.subscriptions_active_count, 0) as returning_customer_subscription_count
from customer_dates as cd
left join subscription_active_dates_agg_customer as sad
    on cd.date = sad.date
    and cd.customer_id = sad.customer_id
