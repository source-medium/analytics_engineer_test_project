-- first step, get a model that is as the customer*day grain for every day they had
-- at least one active subscription.

with customers_days as (

    select
        date,
        customer_id,
        count(subscription_id) as customer_subscriptions_count,
        max(cancelled_date) as cancelled_date

    from {{ ref('subscriptions_days') }}
    group by 1,2
),

customers_days_with_lead as (
    -- use lead() to find the next date that this model shows the customer has an
    -- active subscription.
    select
        *,
        date_diff(
            lead(date) over (
                partition by customer_id
                order by date asc
            ),
            date,
            day
      ) as days_to_next_customer_subscription_day

    from customers_days
),

customers_days_next_day_churn as (
    -- if the customer's next subscription date is more than 1 day beyond the given date
    -- or if the customer doesn't have a next active subscription date, then the subscriber
    -- will churn the following day.
    select
        *,

        case
            when days_to_next_customer_subscription_day > 1 then true
            when days_to_next_customer_subscription_day is null then true
            else false
        end as is_churn_next_day

    from customers_days_with_lead
),

customers_days_churn_date_added as (
    -- based on the churn logic in the prompt, the churn date is the first day
    -- that a subscriber does not have an active subscription, so we derive that
    -- here based on the date in the given row where is_churn_next_day = true.
    select
        *,

        case
            when is_churn_next_day
            then date_add(date, interval 1 day)
            else null
        end as churned_date

    from customers_days_next_day_churn
),

subscriber_cancellations as (

    select
        churned_date as date,
        count(*) as subscribers_cancelled

    from customers_days_churn_date_added
    where is_churn_next_day
    group by 1
)

select * from subscriber_cancellations
