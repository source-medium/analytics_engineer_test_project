-- intent of this model:
---- fan out subscriptions by days between created date and cancelled date to identify
---- all the days where this subscription was active and contributing to the overall
---- active subscriptions count.

select
    date_spine.date_day,
    subscriptions.subscription_id,
    subscriptions.customer_id,

    subscriptions.created_date,
    subscriptions.cancelled_date,

    -- identify the days that the subscription was active
    case
        when subscriptions.cancelled_date is null then true
        when date_day < subscriptions.cancelled_date then true
        when date_day = subscriptions.cancelled_date then false
    end as is_active

from {{ ref('date_spine') }}
left join {{ ref('base__recharge_subscriptions') }} as subscriptions
    on date_spine.date_day >= subscriptions.created_date
    -- not ideal to hard code a date in the future, but placeholding to make this work before refactoring
    and date_spine.date_day <= coalesce(subscriptions.cancelled_date, '2099-01-01')
