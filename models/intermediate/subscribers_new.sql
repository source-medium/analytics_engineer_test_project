select
    created_date as date,
    count(*) as subscribers_new

from {{ ref('base__recharge_subscriptions') }}
where customer_subscription_number = 1
group by 1
