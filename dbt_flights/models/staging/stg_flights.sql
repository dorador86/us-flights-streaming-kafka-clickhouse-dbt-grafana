with source as (
    select * from {{ source('flights_source', 'flights_raw') }}
),

renamed as (
    select
        FlightDate as flight_date,
        Airline as airline,
        Tail_Number as tail_number,
        Origin as origin,
        Dest as dest,
        Cancelled as is_cancelled,
        Diverted as is_diverted,
        DepDelay as dep_delay,
        ArrDelay as arr_delay,
        AirTime as air_time,
        Distance as distance
    from source
)

select * from renamed
