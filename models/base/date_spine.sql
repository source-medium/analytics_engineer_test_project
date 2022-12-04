-- To Do: look into using Jinja to set these parameters using a SQL statement in a macro
-- https://stackoverflow.com/questions/64007239/hi-how-do-we-define-select-statement-as-a-variable-in-dbt
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="'2019-01-01'",
    end_date="'2022-04-08'"
   )
}}
