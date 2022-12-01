select
    sha256(subscription_sku) as primary_key
    , subscription_sku
    , subscription_title
    , subscription_price
    , variant_title
    , shopify_product_id
    , shopify_variant_id
    , recharge_product_id
from {{ ref('staging_acme_recharge_subscriptions')}}