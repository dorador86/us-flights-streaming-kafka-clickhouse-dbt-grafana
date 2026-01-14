{{ config(
    materialized='materialized_view',
    engine='MergeTree()',
    order_by='(flight_date, airport_code)'
) }}

select
    flight_date,
    origin as airport_code,
    count(*) as total_departures,
    -- Performance metrics (SUM used for exact aggregation)
    sum(dep_delay) as tot_dep_delay,
    avg(speed_mph) as avg_speed_outbound,
    -- Count by status categories
    countIf(delay_severity = 'On-Time') as on_time_departures,
    countIf(delay_severity = 'Minor') as minor_delays,
    countIf(delay_severity = 'Moderate') as moderate_delays,
    countIf(delay_severity = 'Critical') as critical_delays,
    countIf(is_cancelled = 1) as cancelled_departures,
    max(processed_at) as last_processed_at
from {{ ref('silver_flights_enriched') }}
group by flight_date, origin
