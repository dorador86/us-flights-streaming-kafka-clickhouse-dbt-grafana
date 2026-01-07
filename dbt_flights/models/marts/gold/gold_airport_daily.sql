{{ config(materialized='table') }}

select
    flight_date,
    origin as airport_code,
    count(*) as total_departures,
    avg(dep_delay) as avg_dep_delay,
    countIf(is_cancelled) as cancelled_departures,
    avg(speed_mph) as avg_speed_outbound
from {{ ref('silver_flights_enriched') }}
group by flight_date, origin
