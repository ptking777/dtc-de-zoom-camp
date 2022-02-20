with green_data as (
    select *, 
        'Green' as service_type 
    from {{ ref('stg_green_tripdata') }}
), 

yellow_data as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 

fhv_data as (
    select 
        tripid,
        vendorid,
        ratecodeid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,

    -- trip info
        cast(NULL AS boolean) as store_and_fwd_flag,
        cast(NULL as integer) as passenger_count,
        cast(NULL as numeric) as trip_distance,
        cast(NULl as integer) as trip_type,

        -- payment info

        cast(NULL as numeric) as fare_amount,
        cast(NULL as numeric) as extra,
        cast(NULL as numeric) as mta_tax,
        cast(NULL as numeric) as tip_amount,
        cast(NULL as numeric) as tolls_amount,
        cast(NULL as numeric) as ehail_fee,
        cast(NULL as numeric) as improvement_surcharge,
        cast(NULL as numeric) as total_amount,
        cast(NULL as integer) as payment_type,
        cast(NULL as integer) as payment_type_description, 
        cast(NULL as numeric) as congestion_surcharge,   
        'FHV' as service_type
        from {{ ref('stg_fhv_tripdata') }}
), 


trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
    union all 
    select * from fhv_data
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description, 
    trips_unioned.congestion_surcharge
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
