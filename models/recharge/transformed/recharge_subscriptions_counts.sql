
{{
  config(
          materialized='table'
        )
}}

with recharge_subscriptions as (

  select * from {{ ref('recharge_subscriptions_xf') }}

), dim_date as (

  select * from {{ ref('dim_date') }}  

), created_ranges as (

  /*
    Getting min and max of dates in the dataset.
    This is used to pass into the date series.
  */  

  select 
      min(subscription_created_date)            as first_created_date
    , max(subscription_created_date)            as last_created_date
  from recharge_subscriptions  
    
), date_series as (

  /*
    Creating an objective date series in order
    to handle different timestamps and fill in
    dates with no activities. 
  */  

  select distinct 
      dim_date.date_actual
  from dim_date
  inner join created_ranges
    on dim_date.date_actual >= created_ranges.first_created_date
    and dim_date.date_actual <= created_ranges.last_created_date
  where 1 = 1

), new_subscriptions as ( 

  /*
    Counting new subscriptions. I separated this
    out due to the different timestamps. Joining
    cancelled_at and created_at to the date_series
    in the same query was producing odd results.
  */  
  
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
  left join recharge_subscriptions
    on date_series.date_actual = recharge_subscriptions.subscription_created_date
  group by 1  

), cancelled_subscriptions as (

  /*
    Counting cancelled subscriptions. I separated 
    this out due to the different timestamps. Joining
    cancelled_at and created_at to the date_series
    in the same query was producing odd results.
  */  

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
  left join recharge_subscriptions
    on date_series.date_actual = recharge_subscriptions.subscription_cancelled_date
  where 1 = 1
  group by 1  

), final as (

  /*
    Joining new_subscriptions and cancelled
    subscriptions together. We can join these
    together, because the new subcriptions has
    the date series. 
  */    

  select
      new_subscriptions.date_actual
    , new_subscriptions.new_subscriptions  
    , new_subscriptions.returning_subscriptions
    , new_subscriptions.new_subscribers
    , cancelled_subscriptions.num_cancelled_subscriptions
    , cancelled_subscriptions.active_cancelled_subscriptions
    , cancelled_subscriptions.passive_cancelled_subscriptions
    , cancelled_subscriptions.same_day_cancels
    , cancelled_subscriptions.num_null_cancel_dates
  from new_subscriptions
  left join cancelled_subscriptions
    on new_subscriptions.date_actual = cancelled_subscriptions.date_actual

)

select * 
from final



