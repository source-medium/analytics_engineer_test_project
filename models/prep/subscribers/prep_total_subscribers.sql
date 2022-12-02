
with active_layer as (
    with base_active_layer as (
        select 
            customer_id 
            , date(created_at) as created_date
        from {{ ref('prep_customer_subscriptions')}}
        where is_subscription_active
    )
    select
        created_date 
        , count(distinct customer_id) as active_subscribers
    from base_active_layer
    group by 
        created_date
)

, cancelled_date_lag as (
    with base_cancelled_date_lag as (
        select distinct
            prep_no_active_subscribers.customer_id 
            , date(prep_customer_subscriptions.cancelled_at) as cancelled_date
            , date_add(date(prep_customer_subscriptions.cancelled_at), interval 1 day) as lag_date
        from {{ ref('prep_customer_subscriptions')}}
        inner join {{ ref('prep_no_active_subscribers') }} on 
            prep_customer_subscriptions.customer_id = prep_no_active_subscribers.customer_id
    )
    select 
        lag_date 
        , count(distinct customer_id) as churned_subscribers
    from base_cancelled_date_lag
    group by 
        lag_date
)

select 
    dim_date.date
    , sum(active_layer.active_subscribers) over (partition by 1 order by active_layer.created_date asc rows between unbounded preceding and current row) as subscribers_active
    , sum(cancelled_date_lag.churned_subscribers) over (partition by 1 order by cancelled_date_lag.lag_date asc rows between unbounded preceding and current row) as subscribers_churned
from {{ ref('dim_date') }} 
left join active_layer on 
    dim_date.date = active_layer.created_date
left join cancelled_date_lag on 
    dim_date.date = cancelled_date_lag.lag_date
order by dim_date.date desc
