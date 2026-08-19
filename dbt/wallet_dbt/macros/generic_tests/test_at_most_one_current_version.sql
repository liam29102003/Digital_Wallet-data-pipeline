{% test at_most_one_current_version(model, column_name, natural_key) %}

select
    {{ natural_key }} as natural_key,
    count(*) as current_version_count
from {{ model }}
where {{ column_name }}
group by {{ natural_key }}
having count(*) > 1

{% endtest %}