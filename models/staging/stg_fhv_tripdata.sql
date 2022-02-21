with fhv_vendors as (
   SELECT dispatching_base_num as base_num, vendorid
  FROM {{ ref('stg_fhv_vendors')}}
),
tripdata as (
  select 
    --dispatching_base_num as vendorid,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    safe_cast(pulocationid as integer) as pickup_locationid,
    safe_cast(dolocationid as integer) as dropoff_locationid,
    IFNULL(sr_flag,"0") AS sr_flag,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  --where dispatching_base_num is not null 
  --and pulocationid != 'PULocationID'
)


select 
   -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    fhv_vendors.vendorid as vendorid,
    dispatching_base_num,
    cast(NULL as integer) as ratecodeid,
    pickup_locationid,
    dropoff_locationid,

        -- timestamps
    safe_cast(pickup_datetime as timestamp) as pickup_datetime,
    safe_cast(dropoff_datetime as timestamp) as dropoff_datetime,
    sr_flag,
    rn
from tripdata
inner join fhv_vendors
on tripdata.dispatching_base_num = fhv_vendors.base_num
--where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}