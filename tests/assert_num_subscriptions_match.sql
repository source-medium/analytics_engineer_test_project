with base_recharge as (
  select
    count(subscription_id)      as tot_subs
  from {{ ref('base_recharge_subscriptions' )}}

), subscription_report as (
  select
    sum(new_subscriptions)      as sum_subs
  from {{ ref('subscription_report' )}}

)    

select *
from base_recharge,subscription_report
where 1 = 1
  and tot_subs != sum_subs
