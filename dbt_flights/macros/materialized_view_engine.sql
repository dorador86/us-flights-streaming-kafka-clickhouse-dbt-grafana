{% macro materialized_view_sql() %}
    {% if target.type == 'clickhouse' %}
        engine = AggregatingMergeTree()
        order by (flight_date, airline, origin)
    {% endif %}
{% endmacro %}
