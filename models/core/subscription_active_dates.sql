
with recharge_subscriptions as (
    select 
        *
      , EXTRACT(date FROM 
                    MAX(cancelled_at) over (partition by null)
       ) as most_recent_subscription_created_date
    from {{ ref('acme_recharge__subscriptions') }}
),

date_spine as (
    select * from {{ ref('utilities__dates') }}
)

select
      {{ dbt_utils.surrogate_key('ds.date_day', 'rs.subscription_id') }} as subscription_active_date_id
    , ds.date_day AS report_date
    , rs.subscription_id
    , rs.created_at
    , rs.cancelled_at
    , rs.customer_id
    , rs.shopify_product_id
    , rs.sku
from date_spine as ds
left join recharge_subscriptions as rs
    ON ds.date_day >= EXTRACT(date from rs.created_at)
    -- Only expand the subscription until its cancellation date or the last created date in the table
    -- TODO: Should we be counting the cancelled at as in force for some period of time?
    -- Defaulting that we should
    AND ds.date_day <= COALESCE(EXTRACT(date from rs.cancelled_at), most_recent_subscription_created_date)
    