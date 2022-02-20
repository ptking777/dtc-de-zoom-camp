with tripdata as 
(
  select *,
    row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
  from {{ source('staging','green_tripdata') }}
  where vendorid is not null 
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    safe_cast(vendorid as integer) as vendorid,
    safe_cast(ratecodeid as integer) as ratecodeid,
    safe_cast(pulocationid as integer) as  pickup_locationid,
    safe_cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    safe_cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    safe_cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    safe_cast(passenger_count as integer) as passenger_count,
    safe_cast(trip_distance as numeric) as trip_distance,
    safe_cast(trip_type as integer) as trip_type,
    
    -- payment info
    safe_cast(fare_amount as numeric) as fare_amount,
    safe_cast(extra as numeric) as extra,
    safe_cast(mta_tax as numeric) as mta_tax,
    safe_cast(tip_amount as numeric) as tip_amount,
    safe_cast(tolls_amount as numeric) as tolls_amount,
    safe_cast(ehail_fee as numeric) as ehail_fee,
    safe_cast(improvement_surcharge as numeric) as improvement_surcharge,
    safe_cast(total_amount as numeric) as total_amount,
    safe_cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    safe_cast(congestion_surcharge as numeric) as congestion_surcharge
from tripdata
where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
