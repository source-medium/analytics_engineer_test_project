select
    customer_id
    , transaction_id
    , subscription_sku
    , subscription_title
    , subscription_price
    , is_subscription_cancelled
    , is_subscription_active
    , is_subscription_prepaid
    , max_retries_reached
    , quantity
    , created_at
    , cancelled_at
    , cancellation_reason
    , cancellation_reason_comments
    , next_charge_scheduled_at
    , expire_after_specific_number_of_charges
from {{ ref('staging_acme_recharge_subscriptions')}}