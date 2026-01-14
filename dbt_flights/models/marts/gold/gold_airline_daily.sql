{{ config(
    materialized='materialized_view',
    engine='MergeTree()',
    order_by='(flight_date, airline)'
) }}

select
    flight_date,
    airline,
    count(*) as total_flights,
    -- Performance metrics (SUM used for exact aggregation)
    sum(dep_delay) as tot_dep_delay,
    max(dep_delay) as max_dep_delay,
    -- Count by status categories
    countIf(delay_severity = 'On-Time') as on_time_flights,
    countIf(delay_severity = 'Minor') as minor_delays,
    countIf(delay_severity = 'Moderate') as moderate_delays,
    countIf(delay_severity = 'Critical') as critical_delays,
    countIf(is_cancelled = 1) as cancelled_flights,
    max(processed_at) as last_processed_at
from {{ ref('silver_flights_enriched') }}
group by flight_date, airline
