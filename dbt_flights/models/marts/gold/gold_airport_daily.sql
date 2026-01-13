{{ config(
    materialized='materialized_view',
    engine='MergeTree()',
    order_by='(flight_date, airport_code)'
) }}

select
    flight_date,
    origin as airport_code,
    count(*) as total_departures,
    -- Métricas de rendimiento (Cambiado a SUM para agregación exacta)
    sum(dep_delay) as tot_dep_delay,
    avg(speed_mph) as avg_speed_outbound,
    -- Conteo por categorías de estado
    countIf(delay_severity = 'On-Time') as on_time_departures,
    countIf(delay_severity = 'Leve') as leve_delays,
    countIf(delay_severity = 'Moderado') as moderado_delays,
    countIf(delay_severity = 'Critico') as critico_delays,
    countIf(is_cancelled = 1) as cancelled_departures,
    max(processed_at) as last_processed_at
from {{ ref('silver_flights_enriched') }}
group by flight_date, origin
