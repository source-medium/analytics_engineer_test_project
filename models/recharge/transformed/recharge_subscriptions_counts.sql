
{{
  config(
          materialized='table'
        )
}}

with recharge_subscriptions as (

  select * from {{ ref('recharge_subscriptions_xf') }}

), dim_date as (

  select * from {{ ref('dim_date') }}  

), subscriber_cancels as (

  select 
      subscription_cancelled_at
    , customer_id 
    , subscription_id  
    , count(
            case 
              when is_subscription_active_cancelled = true
              or is_subscription_payment_failure = true
                then subscription_id
              else null 
            end 
           ) 
        over (
               partition by customer_id 
               order by subscription_cancelled_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_cancelled_subs                                     
    , count(
            case 
              when is_subscription_active_cancelled = true
              and is_same_day_cancel = true
                then subscription_id
              else null 
            end 
           ) 
        over (
               partition by customer_id 
               order by subscription_cancelled_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_same_day_cancelled_subs
    , count(
            case 
              when is_subscription_payment_failure = true
                then subscription_id
              else null 
            end 
           ) 
        over (
               partition by customer_id 
               order by subscription_cancelled_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_passive_cancelled_subs                                                           
  from recharge_subscriptions  
    
), subscriber_creates as (

  select 
      subscription_created_at
    , customer_id 
    , subscription_id  
    , count(subscription_id) 
        over (
               partition by customer_id 
               order by subscription_created_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_created_subs                                     
  from recharge_subscriptions    

), add_subscriber_details as (

  select distinct
      recharge_subscriptions.subscription_id  
    , recharge_subscriptions.customer_id
    , recharge_subscriptions.subscription_created_at
    , recharge_subscriptions.subscription_created_date 
    , recharge_subscriptions.subscription_cancelled_at  
    , recharge_subscriptions.subscription_cancelled_date
    , recharge_subscriptions.is_subscription_active_cancelled  
    , recharge_subscriptions.is_same_day_cancel
    , recharge_subscriptions.is_subscription_payment_failure
    , recharge_subscriptions.has_null_cancellation_date
    , max(subscriber_num_cancelled_subs) 
        over (
               partition by recharge_subscriptions.customer_id 
               order by recharge_subscriptions.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as prior_cancels
    , max(subscriber_num_same_day_cancelled_subs) 
        over (
               partition by recharge_subscriptions.customer_id 
               order by recharge_subscriptions.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as prior_same_day_cancels         
    , max(subscriber_num_created_subs) 
        over (
               partition by recharge_subscriptions.customer_id 
               order by recharge_subscriptions.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as prior_creates         
    , min(recharge_subscriptions.subscription_created_at) 
        over (
               partition by recharge_subscriptions.customer_id 
               order by recharge_subscriptions.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as first_subscription_at                                                                   
  from recharge_subscriptions
  left join subscriber_cancels
    on recharge_subscriptions.customer_id = subscriber_cancels.customer_id
    and recharge_subscriptions.subscription_created_at > subscriber_cancels.subscription_cancelled_at  
  left join subscriber_creates
    on recharge_subscriptions.customer_id = subscriber_creates.customer_id
    and recharge_subscriptions.subscription_created_at > subscriber_creates.subscription_created_at    
    
), final as (

  select 
      add_subscriber_details.* 
    , case 
        when prior_cancels = prior_creates 
          then true 
        else false 
      end                                               as is_returning_subscription
    , case 
        when first_subscription_at = subscription_created_at 
          then true 
        else false 
      end                                               as is_first_subscription           
  from add_subscriber_details
)

select * 
from final
--from final


