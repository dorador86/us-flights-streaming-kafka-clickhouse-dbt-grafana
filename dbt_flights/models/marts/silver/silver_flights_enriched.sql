{{ config(
    materialized='materialized_view',
    engine='MergeTree()',
    order_by='(flight_date, airline, origin)'
) }}

with flight_data as (
    select 
        FlightDate as flight_date,
        Airline as airline,
        Tail_Number as tail_number,
        Origin as origin,
        Dest as dest,
        Cancelled as is_cancelled,
        Diverted as is_diverted,
        if(isFinite(DepDelay), DepDelay, NULL) as dep_delay,
        if(isFinite(ArrDelay), ArrDelay, NULL) as arr_delay,
        if(isFinite(AirTime), AirTime, NULL) as air_time,
        Distance as distance
    from {{ source('flights_source', 'flights_raw') }}
)

select
    *,
    
    -- Calcula la Gravedad del Retraso
    case
        when is_cancelled = 1 then 'Cancelado'
        when dep_delay <= 0 then 'On-Time' 
        when dep_delay > 0 and dep_delay <= 15 then 'Leve'
        when dep_delay > 15 and dep_delay <= 45 then 'Moderado'
        when dep_delay > 45 then 'Critico'
        else 'Sin Datos'
    end as delay_severity,

    -- Calcula Velocidad Media (Millas por Hora)
    case 
        when air_time > 0 then (distance / (air_time / 60))
        else 0 
    end as speed_mph,

    case
        when dep_delay > 0 and arr_delay is not null then (dep_delay - arr_delay)
        else 0
    end as time_recovered,
    
    now() as processed_at
    
from flight_data
