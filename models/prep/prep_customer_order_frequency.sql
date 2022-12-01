select
    sha256(customer_id) as primary_key
    , subscription_sku
    , order_day_of_week
    , order_day_of_month
    , order_interval_unit
    , order_interval_frequency
    , charge_interval_frequency
from {{ ref('staging_acme_recharge_subscriptions')}}