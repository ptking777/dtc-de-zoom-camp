with trip_data as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
) 
-- , fhv_vendors as (
--    SELECT *
--    FROM {{ ref('stg_fhv_vendors') }}
--
--)

select 
    trip_data.tripid, 
    trip_data.vendorid,
    trip_data.dispatching_base_num,
    trip_data.service_type,
    trip_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trip_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    trip_data.pickup_datetime, 
    trip_data.dropoff_datetime, 
    trip_data.sr_flag     
from trip_data
inner join dim_zones as pickup_zone
on trip_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trip_data.dropoff_locationid = dropoff_zone.locationid
--where trip_data.pickup_locationid is not null