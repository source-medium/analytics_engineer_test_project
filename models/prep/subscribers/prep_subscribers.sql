with first_created_date as (
    select
        customer_id 
        , min(date(created_at)) as first_created_date
    from {{ ref('prep_customer_subscriptions') }}
    where is_subscription_active
    group by 
        customer_id
)
, no_active_subscribers as (
    select 
        customer_id 
    from {{ ref('prep_customer_subscriptions') }}
    where is_subscription_cancelled
    except distinct 
    select 
        customer_id 
    from {{ ref('prep_customer_subscriptions') }}
    where is_subscription_active
)

, first_cancelled_date as (
    select
        no_active_subscribers.customer_id 
        , min(date(prep.cancelled_at)) as first_cancelled_date
    from no_active_subscribers
    left join {{ ref('prep_customer_subscriptions') }} as prep on 
        no_active_subscribers.customer_id = prep.customer_id
    group by 
        customer_id
)

select 
    dim_date.date 
    , count(distinct first_created_date.customer_id) as subscribers_new
    , count(distinct first_cancelled_date.customer_id) as subscribers_cancelled
from {{ ref('dim_date') }} 
left join first_created_date on 
    dim_date.date = first_created_date.first_created_date 
left join first_cancelled_date on
    dim_date.date = first_cancelled_date.first_cancelled_date
group by 
    dim_date.date 
order by 
    dim_date.date  desc