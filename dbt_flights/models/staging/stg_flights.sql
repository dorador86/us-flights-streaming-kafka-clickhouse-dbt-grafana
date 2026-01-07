with source as (
    select * from {{ source('flights_source', 'flights_raw') }}
),

renamed as (
    select
        FlightDate as flight_date,
        Airline as airline,
        Origin as origin,
        Dest as dest,
        Cancelled as is_cancelled,
        Diverted as is_diverted,
        DepDelay as dep_delay,
        -- ArrDelay as arr_delay, -- Missing in source
        AirTime as air_time,
        Distance as distance,
        -- Extra fields if available
        Marketing_Airline_Network as marketing_airline,
        OriginCityName as origin_city,
        DestCityName as dest_city
    from source
)

select * from renamed
