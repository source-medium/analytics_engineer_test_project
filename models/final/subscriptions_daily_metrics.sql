with joined as (

    select
        coalesce(
            subscriptions_new.date,
            subscriptions_cancelled.date
        ) as date,

        subscriptions_new.subscriptions_new,
        subscriptions_cancelled.subscriptions_cancelled

    from {{ ref('subscriptions_new') }}
    full outer join {{ ref('subscriptions_cancelled') }}
        on subscriptions_new.date = subscriptions_cancelled.date
)

select * from joined
