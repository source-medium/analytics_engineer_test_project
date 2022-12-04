-- this model gets the daily subscriptions cancelled count, then date spines this
-- data to fill in any cases where there were no cancellations in a given day,
-- then uses a sum window function to get the running total of how many subscriptions
-- have been cancelled up to a given date.

with subscriptions_cancelled as (
    select
        cancelled_date as date,
        count(*) as subscriptions_cancelled

    from {{ ref('base__recharge_subscriptions') }}
    where was_cancelled = true
    group by 1
),

subscriptions_cancelled_date_spine as (
    -- spine allows for days with no cancellations to be coalesced to 0 and therefore
    -- incorporated into the sum() over () window function following this cte
    select
        subscriptions_cancelled.date,
        coalesce(subscriptions_cancelled.subscriptions_cancelled,0) as subscriptions_cancelled

    from subscriptions_cancelled
    left join {{ ref('date_spine') }}
        on subscriptions_cancelled.date = date_spine.date_day
)

select
    *,
    sum(subscriptions_cancelled) over (order by date asc) as subscriptions_churned

from subscriptions_cancelled_date_spine
