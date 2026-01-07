{{ config(materialized='table') }}

with flight_data as (
    select * from {{ ref('stg_flights') }}
)

select
    *,
    
    -- Calcula la Gravedad del Retraso
    case
        when dep_delay <= 15 then 'Leve'
        when dep_delay > 15 and dep_delay <= 45 then 'Moderado'
        when dep_delay > 45 then 'Critico'
        else 'Sin Datos'
    end as delay_severity,

    -- Calcula Velocidad Media (Millas por Hora)
    case 
        when air_time > 0 then (distance / (air_time / 60))
        else 0 
    end as speed_mph

    -- TODO: Recovery Efficiency calculation requires ArrDelay, which is currently missing from ingestion.
    -- , (dep_delay - arr_delay) as time_recovered
    
from flight_data
