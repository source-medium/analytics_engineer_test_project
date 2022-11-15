select
    coalesce(
        subscriptions_new.date,
        subscriptions_cancelled.date
    ) as date,

    subscriptions_new.subscriptions_new,
    subscriptions_cancelled.subscriptions_cancelled,
    subscriptions_active.subscriptions_active,

    subscribers_new.subscribers_new,
    subscribers_active.subscribers_active

from {{ ref('subscriptions_new') }}
full outer join {{ ref('subscriptions_cancelled') }}
    on subscriptions_new.date = subscriptions_cancelled.date
left join {{ ref('subscriptions_active') }}
    on subscriptions_new.date = subscriptions_active.date
left join {{ ref('subscribers_new') }}
    on subscriptions_new.date = subscribers_new.date
full outer join {{ ref('subscribers_active') }}
    on subscriptions_new.date = subscribers_active.date
