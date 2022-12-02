select 
    customer_id 
from {{ ref('prep_customer_subscriptions') }}
where is_subscription_cancelled
except distinct 
select 
    customer_id 
from {{ ref('prep_customer_subscriptions') }}
where is_subscription_active