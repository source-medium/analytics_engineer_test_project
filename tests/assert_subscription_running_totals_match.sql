{{
  config(
         enabled = false
        )
}}

with max_date as (
  select
    max(date_actual)                               as max_date
  from {{ ref('subscription_report' ) }}

), base_recharge as (
  select
      count(distinct subscription_id)               as tot_base_subscriptions
    , count(distinct 
            case 
              when subscription_status = 'cancelled'
              and  subscription_cancelled_at is null
                then subscription_id
              else null
            end 
           )                                        as tot_null_canceled_subscriptions
    , count(distinct 
            case 
              when subscription_status = 'cancelled'
              and subscription_cancellation_reason like '%max number of charge attempts%' 
                then subscription_id
              else null
            end 
           )                                        as tot_payment_failure_subscriptions               
  from {{ ref('base_recharge_subscriptions' ) }}

), subscription_report as (
  select
      subscription_report.active_subscriptions + 
      subscription_report.churned_subscriptions      as total_report_subscriptions
  from {{ ref('subscription_report') }} subscription_report
  inner join max_date 
    on subscription_report.date_actual = max_date.max_date

)    

select *
from base_recharge,subscription_report
where 1 = 1
  and (total_report_subscriptions - tot_null_canceled_subscriptions - tot_payment_failure_subscriptions) != tot_base_subscriptions
