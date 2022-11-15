select
    date_spine.date_day as date,

    coalesce(subscriptions_new.subscriptions_new) as subscriptions_new,
    coalesce(subscriptions_returning.subscriptions_returning) as subscriptions_returning,
    coalesce(subscriptions_cancelled.subscriptions_cancelled) as subscriptions_cancelled,
    coalesce(subscriptions_active.subscriptions_active) as subscriptions_active,

    coalesce(subscribers_new.subscribers_new) as subscribers_new,
    coalesce(subscribers_active.subscribers_active) as subscribers_active

from {{ ref('date_spine') }}
left join {{ ref('subscriptions_new') }}
    on date_spine.date_day = subscriptions_new.date
left join {{ ref('subscriptions_returning') }}
    on date_spine.date_day = subscriptions_returning.date
left join {{ ref('subscriptions_cancelled') }}
    on date_spine.date_day = subscriptions_cancelled.date
left join {{ ref('subscriptions_active') }}
    on date_spine.date_day = subscriptions_active.date
left join {{ ref('subscribers_new') }}
    on date_spine.date_day = subscribers_new.date
left join {{ ref('subscribers_active') }}
    on date_spine.date_day = subscribers_active.date
