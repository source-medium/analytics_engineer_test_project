select 
    sha256(cast(recharge_subscriptions.id as string)) as primary_key
    , cast(recharge_subscriptions.id as int) as transaction_id
    , cast(recharge_subscriptions.customer_id as int) as customer_id
    , cast(recharge_subscriptions.sku as string) as subscription_sku
    , cast(recharge_subscriptions.email as string) as customer_email
    , cast(recharge_subscriptions.product_title as string) as subscription_title
    , cast(recharge_subscriptions.price as float64) as subscription_price
    , case 
        when recharge_subscriptions.status = 'CANCELLED' then TRUE
        else FALSE 
    end as is_subscription_cancelled
    , case 
        when recharge_subscriptions.status = 'ACTIVE' then TRUE 
        else FALSE 
    end as is_subscription_active
    , cast(recharge_subscriptions.quantity as int) as quantity
    , cast(recharge_subscriptions.created_at as datetime) as created_at
    , cast(recharge_subscriptions.is_prepaid as bool) as is_subscription_prepaid
    , cast(recharge_subscriptions.cancelled_at as datetime) as cancelled_at
    , coalesce(
        recharge_subscriptions.order_day_of_week
        , extract(dayofweek from date(recharge_subscriptions.created_at))
    ) as order_day_of_week
    , case
        when recharge_subscriptions.has_queued_charges = 1 then TRUE 
        when recharge_subscriptions.has_queued_charges = 0 then FALSE 
    end as has_queued_charges
    , coalesce(
        recharge_subscriptions.order_day_of_month
        , extract(day from date(recharge_subscriptions.created_at))
    ) as order_day_of_month
    , cast(recharge_subscriptions.cancellation_reason as string) as cancellation_reason
    , cast(recharge_subscriptions.max_retries_reached as int) as max_retries_reached
    , case
        when recharge_subscriptions.order_interval_unit = 'year' then 'yearly'
        when recharge_subscriptions.order_interval_unit = 'month' then 'monthly'
        when recharge_subscriptions.order_interval_unit = 'week' then 'weekly'
        when recharge_subscriptions.order_interval_unit = 'day' then 'daily'
        when recharge_subscriptions.order_interval_unit is null then 'N/A'
        else 'ERROR'
    end as order_interval_unit
    , cast(recharge_subscriptions.next_charge_scheduled_at as datetime) as next_charge_scheduled_at
    , cast(recharge_subscriptions.order_interval_frequency as int) as order_interval_frequency
    , cast(recharge_subscriptions.charge_interval_frequency as int) as charge_interval_frequency
    , cast(recharge_subscriptions.cancellation_reason_comments as string) as cancellation_reason_comments
    , cast(recharge_subscriptions.expire_after_specific_number_of_charges as int) as expire_after_specific_number_of_charges
from `wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions` as recharge_subscriptions