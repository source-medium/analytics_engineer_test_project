
with recharge_subscriptions as (
    select 
          *
        , MAX(created_date) over (partition by null) as most_recent_subscription_created_date_all_customers
    from {{ ref('acme_recharge__subscriptions') }}
    where not is_duplicate_subscription
),

date_spine as (
    select * from {{ ref('utilities__dates') }}
)

select
      {{ dbt_utils.surrogate_key('ds.date_day', 'rs.subscription_id') }} as subscription_active_date_id
    , ds.date_day AS date
    , rs.subscription_id
    , rs.created_at
    , rs.cancelled_at
    , rs.customer_id
    , rs.shopify_product_id
    , rs.sku
    , if(rs.created_date = ds.date_day, 1, 0) as is_new_subscription_int
    , if(rs.cancelled_date = ds.date_day, 1, 0) as is_cancelled_subscription_int
    , rs.most_recent_subscription_created_date_all_customers
from date_spine as ds
left join recharge_subscriptions as rs
    ON ds.date_day >= created_date
    -- Only expand the subscription until its cancellation date or the last created date in the table
    -- TODO: Should we be counting the cancelled at as in force for some period of time?
    -- Defaulting that we should
    AND ds.date_day <= COALESCE(rs.cancelled_date, most_recent_subscription_created_date_all_customers)
