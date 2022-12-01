select
    sha256(customer_id) as primary_key
    , customer_id
    , customer_email
from {{ ref('staging_acme_recharge_subscriptions')}}