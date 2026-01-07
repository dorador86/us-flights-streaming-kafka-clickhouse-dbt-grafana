{{ config(materialized='table') }}

select
    flight_date,
    airline,
    count(*) as total_flights,
    avg(dep_delay) as avg_dep_delay,
    max(dep_delay) as max_dep_delay,
    countIf(is_cancelled) as cancelled_flights,
    countIf(delay_severity = 'Leve') as leve_delays,
    countIf(delay_severity = 'Moderado') as moderado_delays,
    countIf(delay_severity = 'Critico') as critico_delays
from {{ ref('silver_flights_enriched') }}
group by flight_date, airline
