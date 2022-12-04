{% set import_acme = select_table('wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions','wise-weaver-282922.masterset_3.acme_sample') %}

with subscriptions_new as (
select
    date(created_at) as created_date
    ,count(id) as new_subscriptions
from
    {{import_acme}} --`wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions`
group by
    created_date
)

,setup as (
select
    *
    ,case
        when status = 'ACTIVE'
            then created_at
        when status = 'CANCELLED'
            then cancelled_at
        else null
    end as rec_start_at
    ,timestamp(current_date()) as rec_end_at
from
    {{import_acme}} --`wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions`
)

,setup_returning as (
select
    *
    ,sum(
        case
            when status = 'ACTIVE'
                then 1
            else 0
        end
        ) over (partition by customer_id order by rec_start_at) as active_cumsum
    ,sum(
        case
            when status = 'CANCELLED'
                then 1
            else 0
        end
        ) over (partition by customer_id order by rec_start_at) as cancelled_cumsum
from
    setup
)

,setup_returning_min_cancel as (
select
    customer_id
    ,min(cancelled_at) as min_cancelled_at
from
    {{import_acme}} --`wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions`
where
    status = 'CANCELLED'
group by
    customer_id
)

,setup_returning_2 as (
select
    sr.*
    ,srmc.min_cancelled_at
    ,lag(sr.active_cumsum) over (partition by sr.customer_id order by sr.rec_start_at) as previous_active_cumsum
    ,lag(sr.cancelled_cumsum) over (partition by sr.customer_id order by sr.rec_start_at) as previous_cancelled_cumsum
from
    setup_returning as sr
left join
    setup_returning_min_cancel as srmc
        on sr.customer_id = srmc.customer_id
)

/* 
The below necessary for the blocks of actives that get activated at the same time but won't have that trend found with just the cumsums and lags 
*/

,setup_returning_3 as (
select
    *
    ,rank() over (partition by customer_id order by created_at) as active_sub_grouping
from
    setup_returning_2
where
    cancelled_cumsum = previous_cancelled_cumsum
)

,subscriptions_returning as (
select
    date(created_at) as created_date
    ,count(id) as returning_subscriptions
from
    setup_returning_3
where
    active_sub_grouping = 1
    and cancelled_cumsum > 0 -- Probably not a necessary condition...
    and min_cancelled_at <= created_at
group by
    date(created_at)
)

/*
The lingering discrepancy for the returning subscriptions metric can probably be resolved by building a cdc history table or instrumenting snapshot tables
*/

,setup_min_max_day as (
select
    min(created_at) as min_created_at
    ,max(created_at) as max_created_at
from
    {{import_acme}} --`wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions`
)

,min_max_day as (
select
    date_day
from
    {{ref('ref_date')}}
where
    date_day >= date((select min_created_at from setup_min_max_day))
    and date_day <= date((select max_created_at from setup_min_max_day))
)

,subscriptions_active as (
select
    mmd.date_day
    ,count(s.id) as active_subscriptions
from
    min_max_day as mmd
left join
    setup as s
        on mmd.date_day between date(s.rec_start_at) and date(s.rec_end_at)
where
    s.status = 'ACTIVE'
group by
    mmd.date_day
)

,subscriptions_cancelled as (
select
    date(cancelled_at) as cancelled_date
    ,count(id) as cancelled_subscriptions
from
    {{import_acme}} --`wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions`
where
    status = 'CANCELLED'
group by
    date(cancelled_at)
)

,subscriptions_churned as (
select
    mmd.date_day
    ,count(s.id) as churned_subscriptions
from
    min_max_day as mmd
left join
    setup as s
        on mmd.date_day between date(s.rec_start_at) and date(s.rec_end_at)
where
    s.status = 'CANCELLED'
group by
    mmd.date_day
)

,setup_subscribers_new as (
select
    customer_id
    ,min(date(created_at)) as cohort_date
from
    {{import_acme}} --`wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions`
group by
    customer_id
)

,subscribers_new as (
select
    mmd.date_day
    ,count(ssn.customer_id) as new_subscribers
from
    min_max_day as mmd
left join
    setup_subscribers_new as ssn
        on mmd.date_day = ssn.cohort_date
group by
    mmd.date_day
)

,setup_subscribers_active_churned as (
select
    mmd.date_day
    ,s.customer_id
    ,sum(
        case
            when status = 'ACTIVE'
                then 1
            else 0
        end
        ) as number_active_subs
from
    min_max_day as mmd
left join
    setup as s
        on mmd.date_day between date(s.rec_start_at) and date(s.rec_end_at)
group by
    mmd.date_day
    ,s.customer_id
)

/*
Discrepancy from the expected results below for active and churned subscribers is likely due to the active state of cancellations between created_at and cancelled_at...
*/

,subscribers_active as (
select
    date_day
    ,count(customer_id) as active_subscribers
from
    setup_subscribers_active_churned
where
    number_active_subs > 0
group by
    date_day
)

,setup_subscribers_cancelled as (
select
    date_day as date_day
    ,count(customer_id) as total_cancelled_subscribers
from
    setup_subscribers_active_churned
where
    number_active_subs = 0
group by
    date_day
)

,subscribers_cancelled as (
select
    date_day
    ,total_cancelled_subscribers - lag(total_cancelled_subscribers) over (order by date_day) as cancelled_subscribers
from
    setup_subscribers_cancelled
)

,subscribers_churned as (
select
    date_add(date_day, INTERVAL 1 DAY) as date_day
    ,count(customer_id) as churned_subscribers
from
    setup_subscribers_active_churned
where
    number_active_subs = 0
group by
    date_add(date_day, INTERVAL 1 DAY)
)

select
    rd.date_day as date
    ,ifnull(sn.new_subscriptions,0) as subscriptions_new
    ,ifnull(sr.returning_subscriptions,0) as subscriptions_returning
    ,ifnull(sc.cancelled_subscriptions,0) as subscriptions_cancelled
    ,ifnull(sa.active_subscriptions,0) as subscriptions_active
    ,ifnull(sch.churned_subscriptions,0) as subscriptions_churned
    ,ifnull(snew.new_subscribers,0) as subscribers_new
    ,ifnull(scanc.cancelled_subscribers,0) as subscribers_cancelled
    ,ifnull(sact.active_subscribers,0) as subscribers_active
    ,ifnull(schurn.churned_subscribers,0) as subscribers_churned
from
    {{ref('ref_date')}} as rd
left join
    subscriptions_new as sn
        on rd.date_day = sn.created_date
left join
    subscriptions_returning as sr
        on rd.date_day = sr.created_date
left join
    subscriptions_cancelled as sc
        on rd.date_day = sc.cancelled_date
left join
    subscriptions_active as sa
        on rd.date_day = sa.date_day
left join
    subscriptions_churned as sch
        on rd.date_day = sch.date_day
left join
    subscribers_new as snew
        on rd.date_day = snew.date_day
left join
    subscribers_cancelled as scanc
        on rd.date_day = scanc.date_day
left join
    subscribers_active as sact
        on rd.date_day = sact.date_day
left join
    subscribers_churned as schurn
        on rd.date_day = schurn.date_day
where
    rd.date_day between '2022-04-03' and '2022-04-07'
order by
    rd.date_day desc
