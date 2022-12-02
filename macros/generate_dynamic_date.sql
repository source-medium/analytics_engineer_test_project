{% macro generate_dynamic_date(condition_1, date_1, condition_2, date_2) %}

date(
    case 
        when {{ condition_1 }} then {{ date_1 }}
        when {{ condition_2 }} then {{ date_2 }}
        end
)


{% endmacro %}
