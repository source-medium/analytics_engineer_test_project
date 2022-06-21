
{{
  config(
          materialized='table'
        )
}}

with dim_date as (

  select * from {{ ref('dim_date') }}

), fct_recharge_subscriptions as (

  select * from {{ ref('recharge_subscriptions_counts') }}

), daily_customer_status as (

  select * from {{ ref('recharge_subscribers_counts') }}  

), created_ranges as (

  select 
      min(subscription_created_date)            as first_created_date
    , max(subscription_created_date)            as last_created_date
  from fct_recharge_subscriptions  
    
), date_series as (

  select distinct 
      dim_date.date_actual
  from dim_date
  inner join created_ranges
    on dim_date.date_actual >= created_ranges.first_created_date
    and dim_date.date_actual <= created_ranges.last_created_date
  where 1 = 1

), new_subscriptions as ( 
  
  select 
      date_series.date_actual
    , count(subscription_id)                            as new_subscriptions
    , count(
            case 
              when is_returning_subscription = true
                then subscription_id
              else null 
            end
           )                                            as returning_subscriptions
    , count(distinct
            case 
              when is_first_subscription = true
                then customer_id
              else null 
            end
           )                                            as new_subscribers
  from date_series
  left join fct_recharge_subscriptions
    on date_series.date_actual = fct_recharge_subscriptions.subscription_created_date
  group by 1  

), active_subscriptions as (

  select 
      date_series.date_actual
    , subscription_id                            as new_subscriptions
  from date_series
  left join fct_recharge_subscriptions
    on date_series.date_actual >= fct_recharge_subscriptions.subscription_created_date 
    and date_series.date_actual <= fct_recharge_subscriptions.subscription_cancelled_date 
  

), cancelled_subscriptions as (

  select 
      date_series.date_actual
    , count(
            case
              when is_subscription_active_cancelled = true
              or is_subscription_payment_failure = true 
                then subscription_id
              else null
            end 
           )                                            as num_cancelled_subscriptions    
    , count(
            case
              when is_subscription_active_cancelled = true
                then subscription_id
              else null
            end 
           )                                            as active_cancelled_subscriptions  
    , count(
            case
              when is_subscription_payment_failure = true
                then subscription_id
              else null
            end 
           )                                            as passive_cancelled_subscriptions
    , count(
            case
              when is_same_day_cancel = true 
                then subscription_id
              else null
            end 
           )                                            as same_day_cancels
    , count(
            case
              when has_null_cancellation_date = true 
                then subscription_id
              else null
            end 
           )                                            as num_null_cancel_dates       
  from date_series
  left join fct_recharge_subscriptions
    on date_series.date_actual = fct_recharge_subscriptions.subscription_cancelled_date
  where 1 = 1
  group by 1  


), subscriber_aggs as (

  select 
      date_series.date_actual
    , count(distinct
            case 
              when date_series.date_actual = daily_customer_status.first_subscription_date
                then customer_id 
              else null 
            end 
           )                                             as new_subscribers
    , count( distinct
            case 
              when running_total_active_subs = 0
                then customer_id 
              else null 
            end 
           )                                             as cancelled_subscribers
    , count( distinct
            case 
              when running_total_active_subs > 0
              and prior_total_active_subs = 0
                then customer_id 
              else null 
            end 
           )                                             as returning_subscribers           
  from date_series
  left join daily_customer_status  
    on date_series.date_actual = daily_customer_status.date_actual
  group by 1

), subscriber_lags as(

  select 
      subscriber_aggs.date_actual
    , lag(subscriber_aggs.cancelled_subscribers)
        over(
              order by subscriber_aggs.date_actual
            )                                            as churned_subscribers
  from subscriber_aggs    
    
), joined as (

  select 
      new_subscriptions.date_actual 
    , new_subscriptions.new_subscriptions
    , new_subscriptions.returning_subscriptions
    , cancelled_subscriptions.active_cancelled_subscriptions
    , sum(new_subscriptions.new_subscriptions)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            ) - 
      sum(cancelled_subscriptions.num_cancelled_subscriptions)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            )                                                          as active_subscriptions
    , sum(cancelled_subscriptions.active_cancelled_subscriptions)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            ) -
      sum(new_subscriptions.returning_subscriptions)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            ) -
      sum(cancelled_subscriptions.num_null_cancel_dates)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            )                                                          as churned_subscriptions          
    , new_subscriptions.new_subscribers
    , subscriber_aggs.cancelled_subscribers 
    -- , subscriber_aggs.returning_subscribers 
    , sum(subscriber_aggs.new_subscribers)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            ) +
      sum(subscriber_aggs.returning_subscribers)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            ) -       
      sum(subscriber_aggs.cancelled_subscribers)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            )                                                          as active_subscribers
    , sum(subscriber_lags.churned_subscribers)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            ) -
      sum(subscriber_aggs.returning_subscribers)
        over(
              order by new_subscriptions.date_actual
              rows between unbounded preceding and current row
            )                                                          as churned_subscribers
  from new_subscriptions
  left join cancelled_subscriptions
    on new_subscriptions.date_actual = cancelled_subscriptions.date_actual
  left join subscriber_aggs
    on new_subscriptions.date_actual = subscriber_aggs.date_actual  
  left join subscriber_lags
    on new_subscriptions.date_actual = subscriber_lags.date_actual    

)

select *
from joined
