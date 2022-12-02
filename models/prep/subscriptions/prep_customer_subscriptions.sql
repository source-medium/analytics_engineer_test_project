select
    sha256(concat(
        cast(customer_id as string)
        , cast(created_at as string)
    )) as primary_key
    , customer_id
    , transaction_id
    , subscription_sku
    , subscription_title
    , subscription_price
    , is_subscription_cancelled
    , is_subscription_active
    , is_subscription_prepaid
    , is_skippable
    , is_swappable
    , sku_override
    , max_retries_reached
    , quantity
    , created_at
    , updated_at
    , cancelled_at
    , cancellation_reason
    , cancellation_reason_comments
    , variant_title
    , shopify_product_id
    , shopify_variant_id
    , recharge_product_id
    , next_charge_scheduled_at
    , expire_after_specific_number_of_charges
from {{ ref('staging_acme_recharge_subscriptions')}}