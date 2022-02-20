WITH 
fhv_vendors as (
    SELECT *
    FROM {{ ref('stg_fhv_vendors') }}
)
select * from fhv_vendors
