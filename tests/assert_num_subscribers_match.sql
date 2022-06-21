with base_recharge as (
  select
    count(distinct customer_id)          as tot_subscribers
  from {{ ref('base_recharge_subscriptions') }}

), subscription_report as (
  select
    sum(new_subscribers)                 as sum_subscribers
  from {{ ref('subscription_report') }}

)    

select *
from base_recharge,subscription_report
where 1 = 1
  and tot_subscribers != sum_subscribers
