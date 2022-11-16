with max_created_date as (

    select max(created_date) as max_created_date from {{ ref('base__recharge_subscriptions') }}
),

latest_active_subscription_count as (

    select
        count(*) as count

    from {{ ref('base__recharge_subscriptions') }}
    where status = 'ACTIVE'
),

latest_active_subscription_count_from_date_spine as (

    select
        count(*) as count_from_date_spine

    from {{ ref('subscriptions_days') }}
    inner join max_created_date
        on subscriptions_days.date = max_created_date.max_created_date
    where is_subscription_active
),

cross_joined as (

    select
        *

    from latest_active_subscription_count
    cross join latest_active_subscription_count_from_date_spine
),

meet_condition as (

    select *

    from cross_joined
    where not count = count_from_date_spine
)

select * from meet_condition
