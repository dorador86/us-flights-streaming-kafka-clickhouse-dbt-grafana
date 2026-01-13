{{ config(
    materialized='table'
) }}

with ranked_flights as (
    select
        tail_number,
        flight_date,
        airline,
        origin,
        dest,
        dep_delay,
        arr_delay,
        -- Ordenamos por fecha y (teóricamente por hora si la tuviéramos)
        -- Usamos el orden de inserción/aparición como proxy en este dataset simplificado
        row_number() over (partition by tail_number, flight_date order by origin) as flight_sequence
    from {{ ref('silver_flights_enriched') }}
    where tail_number is not null
      and is_cancelled = 0
)

select
    f1.tail_number,
    f1.flight_date,
    f1.airline,
    f1.origin as first_origin,
    f1.dest as first_dest,
    f1.arr_delay as initial_arrival_delay,
    f2.origin as second_origin,
    f2.dep_delay as subsequent_departure_delay,
    -- Si el primer vuelo llegó tarde (>15min) y el segundo salió tarde
    case 
        when f1.arr_delay > 15 and f2.dep_delay > 15 then 1
        else 0
    end as is_propagation_event
from ranked_flights f1
join ranked_flights f2 
    on f1.tail_number = f2.tail_number 
    and f1.flight_date = f2.flight_date
    and CAST(f1.flight_sequence, 'Int64') = CAST(f2.flight_sequence - 1, 'Int64')
