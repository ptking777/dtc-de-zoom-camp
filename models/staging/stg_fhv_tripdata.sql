with tripdata as 
(
  select 
    dispatching_base_num as vendorid,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    IFNULL(sr_flag,"0") AS sr_flag,
    row_number() over(partition by cast(substring(dispatching_base_num,2) as integer), pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
  and pulocationid != 'PULocationID'
)

select 
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'pickup_datetime']) }} as tripid,
    vendorid,
    cast(NULL as integer) as ratecodeid,
    pickup_locationid,
    dropoff_locationid,

        -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    sr_flag
from tripdata
--where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}