
{{
  config(
          materialized='table'
        )
}}

with recharge_subscriptions as (

  select * from {{ ref('recharge_subscriptions_xf') }}

), dim_date as (
 
  select * from {{ ref('dim_date') }}

), union_event_dates as (

  select 
      subscription_created_date                                     as date_actual
    , customer_id 
  from recharge_subscriptions

  union distinct

  select
      subscription_cancelled_date
    , customer_id 
  from recharge_subscriptions
  where 1 = 1
    and is_subscription_active_cancelled = TRUE
    
    
), customer_aggs as (

  select 
      union_event_dates.date_actual
    , union_event_dates.customer_id                                 
    , count(created_subscription.subscription_id)                   as created_subscriptions
    , count(
            case
              when cancelled_subscription.is_subscription_active_cancelled = true 
                then cancelled_subscription.subscription_id
              else null 
            end 
           )                                                        as active_cancelled_subscriptions 
    , count(
            case
              when cancelled_subscription.is_subscription_payment_failure = true 
                then cancelled_subscription.subscription_id
              else null 
            end 
           )                                                        as payment_failure_subscriptions               
    , count(
            case
              when cancelled_subscription.is_same_day_cancel = true 
                then cancelled_subscription.subscription_id
              else null 
            end 
           )                                                        as same_day_cancelled_subscription 
    , min(created_subscription.subscription_created_date)           as subscription_created_date
  from union_event_dates 
  left join recharge_subscriptions created_subscription
    on union_event_dates.date_actual = created_subscription.subscription_created_date
    and union_event_dates.customer_id = created_subscription.customer_id  
  left join recharge_subscriptions cancelled_subscription
    on union_event_dates.date_actual = cancelled_subscription.subscription_cancelled_date   
    and union_event_dates.customer_id = cancelled_subscription.customer_id
    and cancelled_subscription.is_subscription_active_cancelled = true
  group by 1,2  

), customer_windows as (

  select 
      date_actual
    , customer_id
    , customer_aggs.created_subscriptions    
    , customer_aggs.active_cancelled_subscriptions 
    , customer_aggs.payment_failure_subscriptions    
    , first_value(subscription_created_date)
        over (
               partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
             )                                                  as first_subscription_date
    , sum(customer_aggs.created_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
            )                                                   as running_total_created_subs     
    , sum(customer_aggs.active_cancelled_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
            )                                                   as running_total_active_canceled_subs
    , sum(customer_aggs.payment_failure_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
            )                                                   as running_total_payment_failure_subs         
    , sum(customer_aggs.same_day_cancelled_subscription) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
            )                                                   as running_total_non_same_day_canceled_subs          
    , sum(customer_aggs.created_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
            )  -
      sum(customer_aggs.active_cancelled_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
            ) -
      sum(customer_aggs.payment_failure_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and current row
            )                                                  as running_total_active_subs     
    , sum(customer_aggs.created_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and 1 preceding
            )  -
      sum(customer_aggs.active_cancelled_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and 1 preceding
            ) -
      sum(customer_aggs.payment_failure_subscriptions) 
        over( 
              partition by customer_id
              order by date_actual
              rows between unbounded preceding and 1 preceding
            )                                                  as prior_total_active_subs                       
  from customer_aggs

)

select *
from customer_windows
        