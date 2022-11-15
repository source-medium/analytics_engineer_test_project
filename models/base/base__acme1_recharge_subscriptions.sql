-- the intent of this base model is to logically organize the columns in the source
-- table for readability and do some light cleaning + re-aliasing where necessary.

select
    id,
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

    -- timestamps
    created_at,
    updated_at,
    cancelled_at

from {{ source('raw_data_sandbox', 'acme1_recharge_subscriptions') }}
