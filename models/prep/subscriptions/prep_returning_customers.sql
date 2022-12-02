with cancelled_subscriptions as (
select 
    customer_id 
    , date(cancelled_at) as cancelled_on
    , transaction_id
    , subscription_sku
from {{ ref('prep_customer_subscriptions') }}
where is_subscription_cancelled
)

select 
    prep.customer_id 
    , date(prep.created_at) as created_on
    , prep.transaction_id
    , prep.subscription_sku
from {{ ref('prep_customer_subscriptions') }} as prep
left join cancelled_subscriptions on 
    prep.customer_id = cancelled_subscriptions.customer_id 
where 
    date(prep.created_at) > cancelled_subscriptions.cancelled_on

    