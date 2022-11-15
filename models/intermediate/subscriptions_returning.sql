with base_lagged as (

    select
        *,
        lag(was_cancelled) over (

            partition by customer_id, recharge_product_id
            order by created_at
        ) as was_previous_subscription_cancelled

    from {{ ref('base__recharge_subscriptions') }}
)

select
    date(created_at) as date,
    count(*) as subscriptions_returning

from base_lagged
where was_previous_subscription_cancelled = true
group by 1
