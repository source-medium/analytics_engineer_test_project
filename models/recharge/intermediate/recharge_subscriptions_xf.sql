{{
  config(
          materialized='view'
        )
}}

with recharge_subscriptions as (

  select * from {{ ref('base_recharge_subscriptions') }}

), subscription_properties_parsed as (

  select  
      subscription_id
    , max(
        case 
          when json_value(subscription_properties,'$.name') = 'shipping_interval_frequency'
            then json_value(subscription_properties,'$.value')
          else null 
        end 
      )                                                 as subscription_shipping_interval_frequency
    , max(
        case 
          when json_value(subscription_properties,'$.name') = 'shipping_interval_unit_type'
            then json_value(subscription_properties,'$.value')
          else null 
        end 
      )                                                 as shipping_interval_unit_type  
    , max(
        case 
          when json_value(subscription_properties,'$.name') = 'add_on_subscription_id'
            then json_value(subscription_properties,'$.value')
          else null 
        end 
      )                                                 as add_on_subscription_id
    , max(
        case 
          when json_value(subscription_properties,'$.name') = 'add_on'
            then json_value(subscription_properties,'$.value')
          else null
        end 
      )                                                 as is_add_on    
    , max(
        case 
          when json_value(subscription_properties,'$.name') = '_Kit Notes'
            then json_value(subscription_properties,'$.value')
          else null 
        end 
      )                                                 as kit_notes         
    
  from recharge_subscriptions,  
  unnest(subscription_properties) subscription_properties
  group by 1

), subscription_analytics_data_parsed as (

  select 
      subscription_id
    , max(
        json_value(
                     subscription_utm_params
                    ,'$.utm_data_source'
                  )
      )                                                 as utm_data_source
    , max(
        json_value(
                     subscription_utm_params
                    ,'$.utm_source'
                  )
      )                                                 as utm_source
    , max(
        json_value(
                     subscription_utm_params
                    ,'$.utm_medium'
                  )
      )                                                 as utm_medium
    , max(
        json_value(
                     subscription_utm_params
                    ,'$.utm_campaign'
                  )
      )                                                 as utm_campaign
    , max(
        json_value(
                     subscription_utm_params
                    ,'$.utm_content'
                  ) 
      )                                                 as utm_content
    , max(
        json_value(
                     subscription_utm_params
                    ,'$.utm_timestamp'
                  )
      )                                                 as utm_timestamp              
  from recharge_subscriptions,
  unnest(subscription_utm_params) subscription_utm_params
  group by 1


), subscription_details as (

  select 
      recharge_subscriptions.* except (subscription_cancelled_at,subscription_cancelled_date)
    , case 
        when subscription_status = 'cancelled'
          then coalesce(
                          subscription_cancelled_at
                        , subscription_updated_at
                       )                        
        else null
      end                                               as subscription_cancelled_at
    , case 
        when subscription_status = 'cancelled'
          then coalesce(
                          subscription_cancelled_date
                        , subscription_updated_date
                       )                        
        else null
      end                                               as subscription_cancelled_date
    , case 
        when subscription_status = 'cancelled'
         and subscription_cancelled_at is null
          then true                      
        else false
      end                                               as has_null_cancellation_date  
    , case 
        when subscription_status = 'cancelled'
        and (subscription_cancellation_reason not like '%max number of charge attempts%'
             or subscription_cancellation_reason is null 
            )
          then TRUE 
        else FALSE
      end                                               as is_subscription_active_cancelled
    , case 
        when subscription_status = 'cancelled'
        and subscription_cancellation_reason like '%max number of charge attempts%'
          then TRUE 
        else FALSE
      end                                               as is_subscription_payment_failure
    , case 
        when subscription_status = 'active'
          then TRUE 
        else FALSE
      end                                               as is_subscription_active 
    , case 
        when subscription_created_date = subscription_cancelled_date 
          then TRUE 
        else FALSE
      end                                               as is_same_day_cancel
  from recharge_subscriptions  

), final as (

  select 
      subscription_details.* except(
                                       subscription_properties
                                     , subscription_utm_params
                                   )
    , subscription_properties_parsed.* except(
                                               subscription_id,is_add_on
                                             )
    , case 
        when is_add_on = 'True'
          then TRUE 
        else FALSE 
      end                                               as is_add_on
    , subscription_analytics_data_parsed.* except(
                                                   subscription_id
                                                 )
  from subscription_details
  left join subscription_properties_parsed
    on subscription_details.subscription_id = subscription_properties_parsed.subscription_id
  left join subscription_analytics_data_parsed
    on subscription_details.subscription_id = subscription_analytics_data_parsed.subscription_id   

)

select *
from final 
