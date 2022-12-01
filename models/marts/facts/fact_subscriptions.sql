select 
    dim_date.date
    , count(distinct
        case 
            when new_customers.is_subscription_active then new_customers.transaction_id
        end
    ) as subscriptions_new
    , count(distinct returning_customers.transaction_id) as subscriptions_returning
    , count(distinct
        case 
            when cancelled_customers.is_subscription_cancelled then cancelled_customers.transaction_id
        end
    ) as subscriptions_cancelled
from {{ ref('dim_date') }}
left join {{ ref('prep_customer_subscriptions') }} as new_customers on 
    dim_date.date = date(new_customers.created_at)
left join {{ ref('prep_customer_subscriptions') }} as cancelled_customers on 
    dim_date.date = date(cancelled_customers.cancelled_at)
left join {{ ref('prep_returning_customers') }} as returning_customers on
    dim_date.date = returning_customers.created_on
group by dim_date.date
order by dim_date.date desc