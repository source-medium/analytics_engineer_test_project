-- this model takes the date-spined subscriptions model and aggregates to the date grain.
-- from there we calculate the number of subscriptions and subscribers that were considered that
-- day.
select
    date,
    count(distinct customer_id) as subscribers_active

from {{ ref('subscriptions_days') }}
group by 1
