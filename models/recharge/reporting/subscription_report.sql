
{{
  config(
          materialized='table'
        )
}}

with dim_date as (

  select * from {{ ref('dim_date') }}

), recharge_subscriptions_counts as (

  select * from {{ ref('recharge_subscriptions_counts') }}

), daily_customer_status as (

  select * from {{ ref('recharge_subscribers_counts') }}  

), created_ranges as (

  select 
      min(date_actual)                                   as first_date
    , max(date_actual)                                   as last_date
  from recharge_subscriptions_counts  
    
), date_series as (

  select distinct 
      dim_date.date_actual
  from dim_date
  inner join created_ranges
    on dim_date.date_actual >= created_ranges.first_date
    and dim_date.date_actual <= created_ranges.last_date
  where 1 = 1

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
      date_series.date_actual 
    , recharge_subscriptions_counts.new_subscriptions
    , recharge_subscriptions_counts.returning_subscriptions
    , recharge_subscriptions_counts.active_cancelled_subscriptions
    , sum(recharge_subscriptions_counts.new_subscriptions)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            ) - 
      sum(recharge_subscriptions_counts.num_cancelled_subscriptions)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            )                                                          as active_subscriptions
    , sum(recharge_subscriptions_counts.active_cancelled_subscriptions)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            ) -
      sum(recharge_subscriptions_counts.returning_subscriptions)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            ) -
      sum(recharge_subscriptions_counts.num_null_cancel_dates)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            )                                                          as churned_subscriptions          
    , recharge_subscriptions_counts.new_subscribers
    , subscriber_aggs.cancelled_subscribers 
    -- , subscriber_aggs.returning_subscribers 
    , sum(subscriber_aggs.new_subscribers)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            ) +
      sum(subscriber_aggs.returning_subscribers)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            ) -       
      sum(subscriber_aggs.cancelled_subscribers)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            )                                                          as active_subscribers
    , sum(subscriber_lags.churned_subscribers)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            ) -
      sum(subscriber_aggs.returning_subscribers)
        over(
              order by date_series.date_actual
              rows between unbounded preceding and current row
            )                                                          as churned_subscribers
  from date_series
  left join recharge_subscriptions_counts
    on date_series.date_actual = recharge_subscriptions_counts.date_actual
  left join subscriber_aggs
    on recharge_subscriptions_counts.date_actual = subscriber_aggs.date_actual  
  left join subscriber_lags
    on recharge_subscriptions_counts.date_actual = subscriber_lags.date_actual    

)

select *
from joined
