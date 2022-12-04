with source as (

    select * from {{ source('acme_recharge', 'acme1_recharge_subscriptions') }}

),

renamed as (

    select
        id AS subscription_id,
        shopify_product_id,
        shopify_variant_id,
        recharge_product_id,
        customer_id,
        sku,
        sku_override,
        email customer_email,
        product_title,
        price,
        status AS subscription_status,
        quantity,
        order_interval_frequency,
        order_interval_unit,
        charge_interval_frequency,
        expire_after_specific_number_of_charges,

        created_at,
        updated_at,
        next_charge_scheduled_at,
        cancelled_at,
        cancellation_reason,
        cancellation_reason_comments,

        order_day_of_week,
        order_day_of_month,
        is_prepaid,
        is_skippable,
        is_swappable,
        has_queued_charges,

        properties,
        commit_update,
        variant_title,
        analytics_data,
        max_retries_reached
    from source

)

select * from renamed