-- this model fans out subscriptions by days between created date and cancelled date to identify
-- all the days where this subscription was active and contributing to the overall active
-- subscriptions count. downstream we can aggregate and filter this in multiple ways to calculate
-- different daily metrics.

select
    date_spine.date_day as date,
    subscriptions.subscription_id,
    subscriptions.customer_id,

    subscriptions.created_date,
    subscriptions.cancelled_date,

    -- identify the days that the subscription was active.
    case
        when subscriptions.cancelled_date is null then true
        when date_day < subscriptions.cancelled_date then true
        when date_day = subscriptions.cancelled_date then false
    end as is_subscription_active

from {{ ref('base__recharge_subscriptions') }} as subscriptions
left join {{ ref('date_spine') }}
    on subscriptions.created_date <= date_spine.date_day
    and (
          subscriptions.cancelled_date >= date_spine.date_day
          or subscriptions.cancelled_date is null
        )
