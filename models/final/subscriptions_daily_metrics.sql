select
    coalesce(
        subscriptions_new.date,
        subscriptions_cancelled.date
    ) as date,

    subscriptions_new.subscriptions_new,
    subscriptions_cancelled.subscriptions_cancelled,

    subscribers_new.subscribers_new

from {{ ref('subscriptions_new') }}
full outer join {{ ref('subscriptions_cancelled') }}
    on subscriptions_new.date = subscriptions_cancelled.date
left join {{ ref('subscribers_new') }}
    on subscriptions_new.date = subscribers_new.date
