with max_date as (
  select
    max(date_actual)                               as max_date
  from {{ ref('subscription_report' ) }}

), base_recharge as (
  select
    count(distinct customer_id)                    as tot_base_subscribers
  from {{ ref('base_recharge_subscriptions' ) }}

), subscription_report as (
  select
      subscription_report.cancelled_subscribers + 
      subscription_report.active_subscribers + 
      subscription_report.churned_subscribers       as total_report_subscribers
  from {{ ref('subscription_report') }} subscription_report
  inner join max_date 
    on subscription_report.date_actual = max_date.max_date

)    

select *
from base_recharge,subscription_report
where 1 = 1
  and total_report_subscribers != tot_base_subscribers
