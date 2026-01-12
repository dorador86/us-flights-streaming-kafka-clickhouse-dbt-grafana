{{ config(materialized='table') }}

select
    flight_date,
    origin as airport_code,
    count(*) as total_departures,
    -- Métricas de rendimiento
    avg(dep_delay) as avg_dep_delay,
    avg(speed_mph) as avg_speed_outbound,
    -- Conteo por categorías de estado
    countIf(delay_severity = 'On-Time') as on_time_departures,
    countIf(delay_severity = 'Leve') as leve_delays,
    countIf(delay_severity = 'Moderado') as moderado_delays,
    countIf(delay_severity = 'Critico') as critico_delays,
    countIf(is_cancelled = 1) as cancelled_departures
from {{ ref('silver_flights_enriched') }}
group by flight_date, origin
