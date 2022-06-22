{{
  config(
          materialized='view'
        )
}}

with recharge_subscriptions as (

  select * from {{ ref('base_recharge_subscriptions') }}

), subscription_properties_parsed as (

  /*
    CTE to parse out subscription properties json to columns. 
  */

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

  /*
    CTE to parse out analytics json to columns. 
  */

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

  /*
    CTE to fix some data gaps and add flags to 
    standardidize logic.
  */

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

), subscriber_cancels as (

  /*
    CTE to add window functions for subscriber 
    cancels on each subscription. This enables
    `is_returning_subscription` flag below.
  */

  select 
      subscription_cancelled_at
    , customer_id 
    , subscription_id  
    , count(
            case 
              when is_subscription_active_cancelled = true
              or is_subscription_payment_failure = true
                then subscription_id
              else null 
            end 
           ) 
        over (
               partition by customer_id 
               order by subscription_cancelled_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_cancelled_subs                                     
    , count(
            case 
              when is_subscription_active_cancelled = true
              and is_same_day_cancel = true
                then subscription_id
              else null 
            end 
           ) 
        over (
               partition by customer_id 
               order by subscription_cancelled_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_same_day_cancelled_subs
    , count(
            case 
              when is_subscription_payment_failure = true
                then subscription_id
              else null 
            end 
           ) 
        over (
               partition by customer_id 
               order by subscription_cancelled_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_passive_cancelled_subs                                                           
  from subscription_details  
    
), subscriber_creates as (

  /*
    CTE to add window functions for subscriber 
    creations on each subscription. This enables
    `is_returning_subscription` flag below.
  */

  select 
      subscription_created_at
    , customer_id 
    , subscription_id  
    , count(subscription_id) 
        over (
               partition by customer_id 
               order by subscription_created_at
               rows between unbounded preceding and current row
             )                                          as subscriber_num_created_subs                                     
  from subscription_details    

), join_subscriber_details AS (

  /*
    Join in the subscriber CTEs with subscription
    details for a final table. 
  */  

  select  
      subscription_details.*
    , max(subscriber_num_cancelled_subs) 
        over (
               partition by subscription_details.customer_id 
               order by subscription_details.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as prior_cancels
    , max(subscriber_num_same_day_cancelled_subs) 
        over (
               partition by subscription_details.customer_id 
               order by subscription_details.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as prior_same_day_cancels         
    , max(subscriber_num_created_subs) 
        over (
               partition by subscription_details.customer_id 
               order by subscription_details.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as prior_creates         
    , min(subscription_details.subscription_created_at) 
        over (
               partition by subscription_details.customer_id 
               order by subscription_details.subscription_created_at
               --rows between unbounded preceding and current row
             )                                          as first_subscription_at   
  from subscription_details
  left join subscriber_cancels
    on subscription_details.customer_id = subscriber_cancels.customer_id
    and subscription_details.subscription_created_at > subscriber_cancels.subscription_cancelled_at  
  left join subscriber_creates
    on subscription_details.customer_id = subscriber_creates.customer_id
    and subscription_details.subscription_created_at > subscriber_creates.subscription_created_at             

), final as (

  /*
    Final join bringing everything together
    as a final table.
  */

  select distinct
      join_subscriber_details.* except(
                                       subscription_properties
                                     , subscription_utm_params
                                   )
    , subscription_properties_parsed.* except(
                                               subscription_id,is_add_on
                                             )
    , subscription_analytics_data_parsed.* except(
                                                   subscription_id
                                                 )    
    , case 
        when is_add_on = 'True'
          then TRUE 
        else FALSE 
      end                                               as is_add_on
     , case 
        when join_subscriber_details.prior_cancels = join_subscriber_details.prior_creates 
          then true 
        else false 
      end                                               as is_returning_subscription
    , case 
        when join_subscriber_details.first_subscription_at = join_subscriber_details.subscription_created_at 
          then true 
        else false 
      end                                               as is_first_subscription  

  from join_subscriber_details
  left join subscription_properties_parsed
    on join_subscriber_details.subscription_id = subscription_properties_parsed.subscription_id
  left join subscription_analytics_data_parsed
    on join_subscriber_details.subscription_id = subscription_analytics_data_parsed.subscription_id   

)

select *
from final 
