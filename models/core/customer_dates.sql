
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
        , c.customer_id
    from customers as c
    left join date_spine as ds
        on ds.date_day >= c.initial_subscription_for_customer_date
        and ds.date_day <= c.most_recent_subscription_for_customer_date
)

select
      cd.*
    , sad.subscriptions_active_count
from customer_dates as cd
left join subscription_active_dates_agg_customer as sad
    on cd.date = sad.date
    and cd.customer_id = sad.customer_id
