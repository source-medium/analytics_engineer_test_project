
with prep_layer as (
    select 
        {{ generate_dynamic_date(
            'is_subscription_active'
            ,'created_at'
            ,'is_subscription_cancelled'
            , 'cancelled_at'
        ) }} as dynamic_date
        , count(distinct
            case when is_subscription_active then transaction_id end
        ) as active_subs
        , count(distinct
            case when is_subscription_cancelled then transaction_id end
        ) as cancelled_subs
    from {{ ref('prep_customer_subscriptions')}}
    group by 
        {{ generate_dynamic_date(
            'is_subscription_active'
            , 'created_at'
            , 'is_subscription_cancelled'
            , 'cancelled_at'
        ) }}
)

select 
    dynamic_date 
    , sum(active_subs) over (partition by 1 order by dynamic_date asc rows between unbounded preceding and current row) as subscriptions_active
    , sum(cancelled_subs) over (partition by 1 order by dynamic_date asc rows between unbounded preceding and current row) as subscriptions_churned
from prep_layer 
order by dynamic_date desc
