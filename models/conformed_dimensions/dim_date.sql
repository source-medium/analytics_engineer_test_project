with date_series as (

  select *
  from  unnest(generate_date_array('2010-01-01', '2030-01-01', INTERVAL 1 DAY)) AS date

), date_functions as (

  select
      date_series.date                              as date_actual               -- 2010-01-01
    , format_date('%A',date_series.date)            as day_name                  -- Friday
    , extract(dayofweek from date_series.date)      as day_of_week               -- 6
    , extract(day from date_series.date)            as day_of_month              -- 1
    , extract(month from date_series.date)          as month_actual              -- 1
    , format_date('%B',date_series.date)            as month_name                -- January
    , format_date('%b',date_series.date)            as month_name_abbreviated    -- Jan
    , extract(quarter from date_series.date)        as quarter_actual            -- 1
    , extract(year from date_series.date)           as year_actual               -- 2010
    , date(date_trunc(date_series.date,week))       as first_day_of_week         -- 2009-12-27
    , date(date_trunc(date_series.date,month))      as first_day_of_month        -- 2010-01-01   
    , date(date_trunc(date_series.date,quarter))    as first_day_of_quarter      -- 2010-01-01
    , date(date_trunc(date_series.date,year))       as first_day_of_year         -- 2010-01-01
  from date_series

)

select * from date_functions