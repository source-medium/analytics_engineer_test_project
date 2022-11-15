-- intent of this base model:
---- logically organize the columns in the source table for readability
---- light cleaning + re-aliasing where necessary
---- derive fields that will be helpful downstream

select
    id as subscription_id,
    customer_id,

    -- subscription product details
    shopify_product_id,
    recharge_product_id,
    shopify_variant_id,
    sku,
    product_title,
    price,

    -- status and other info
    status,
    lower(cancellation_reason) as cancellation_reason,

    -- derived

    -- business definition for cancelled subscriptions does not count instances
    -- where cancellation reason is due to max number of charge attempts
    case
        when status = 'CANCELLED'
            and lower(cancellation_reason) not like '%max number of charge attempts%'
        then true
        else false
    end as was_cancelled,

    -- timestamps
    created_at,
    cancelled_at,
    updated_at,

    -- dates
    date(created_at) as created_date,
    date(cancelled_at) as cancelled_date

from {{ source('raw_data_sandbox', 'acme1_recharge_subscriptions') }}
