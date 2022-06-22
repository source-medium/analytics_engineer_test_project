{{
  config(
          materialized='ephemeral'
        )
}}

with source as (

  select * from {{ source('raw_data_sandbox','acme1_recharge_subscriptions') }}

), renamed as (

  select
    -- IDs
      id                                                as subscription_id
    , customer_id                                       as customer_id
    , shopify_product_id                                as shopify_product_id
    , shopify_variant_id                                as shopify_variant_id
    , recharge_product_id                               as recharge_product_id

    --customer details
    , email                                             as customer_email    

    --subscription details
    , lower(order_interval_unit)                        as subscription_interval_unit    
    , order_interval_frequency                          as subscription_interval_frequency
    , charge_interval_frequency                         as charge_interval_frequency
    , price                                             as subscription_price
    , quantity                                          as subscription_quantity
    , lower(cancellation_reason)                        as subscription_cancellation_reason
    , cancellation_reason_comments                      as subscription_cancellation_reason_details  
    , lower(status)                                     as subscription_status
    , properties                                        as subscription_properties
    , json_extract_array(
                           analytics_data
                          ,'$.utm_params'
                        )                               as subscription_utm_params

    --product details
    , sku                                               as product_sku            
    , product_title                                     as product_title
    , variant_title                                     as product_variant_title               

    --timestamps/dates                  
    , created_at                                        as subscription_created_at   
    , updated_at                                        as subscription_updated_at  
    , cancelled_at                                      as subscription_cancelled_at           
    , next_charge_scheduled_at                          as next_charge_scheduled_at
    , date(created_at)                                  as subscription_created_date
    , date(updated_at)                                  as subscription_updated_date    
    , date(cancelled_at)                                as subscription_cancelled_date    
    , date(next_charge_scheduled_at)                    as next_charge_scheduled_date
    , order_day_of_month
    --, extract(day from created_at)                      as order_day_of_month
    , extract(dayofweek from created_at)                as order_day_of_week

     --booleans                  
    , is_prepaid                                        as is_subscription_prepaid
    , is_skippable                                      as is_subscription_skippable
    , is_swappable                                      as is_subscription_swappable
    , sku_override                                      as has_sku_override
    , case                  
        when max_retries_reached = 1                  
          then TRUE                 
        else FALSE                  
      end                                               as has_reached_max_retries
    , case                  
        when has_queued_charges = 1                 
          then TRUE                 
        else FALSE                  
      end                                               as has_queued_charges  
  from source  

)    

select * from renamed