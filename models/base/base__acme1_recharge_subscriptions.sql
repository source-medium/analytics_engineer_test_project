-- the intent of this base model is to logically organize the columns in the source
-- table for readability and do some light cleaning + re-aliasing where necessary.

select
    id as recharge_subscription_id,
    customer_id,

    -- subscription product details
    shopify_product_id,
    recharge_subscription_id,
    shopify_variant_id,
    sku,
    product_title,
    price,

    -- timestamps
    created_at,
    updated_at,
    cancelled_at

from {{ source('raw_data_sandbox', 'acme1_recharge_subscriptions') }}
