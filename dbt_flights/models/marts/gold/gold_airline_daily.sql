{{ config(materialized='table') }}

select
    flight_date,
    airline,
    count(*) as total_flights,
    -- Métricas de rendimiento temporal
    avg(dep_delay) as avg_dep_delay,
    max(dep_delay) as max_dep_delay,
    -- Conteo por categorías de estado (Nueva lógica)
    countIf(delay_severity = 'On-Time') as on_time_flights,
    countIf(delay_severity = 'Leve') as leve_delays,
    countIf(delay_severity = 'Moderado') as moderado_delays,
    countIf(delay_severity = 'Critico') as critico_delays,
    countIf(is_cancelled = 1) as cancelled_flights
from {{ ref('silver_flights_enriched') }}
group by flight_date, airline
