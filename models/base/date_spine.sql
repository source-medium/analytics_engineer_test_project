-- To Do: is there a way to make these variables that accept the min and max of the
-- created dates from the subscriptions table?
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="'2019-01-01'",
    end_date="current_date"
   )
}}
