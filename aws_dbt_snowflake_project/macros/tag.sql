{% macro tag(col) %}
   CASE
        WHEN {{col}} IS NULL THEN 'unknown'
        WHEN {{col}} < 0 THEN 'invalid'
        WHEN {{col}} < 100 THEN 'low'
        WHEN {{col}} < 200 THEN 'medium'
        ELSE 'high'
   END
{% endmacro %}