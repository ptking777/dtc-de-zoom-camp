WITH fhv_base_nums as (
    SELECT dispatching_base_num,
    FROM {{ source('staging','fhv_tripdata') }}
    GROUP BY dispatching_base_num
    ORDER BY 1 DESC
),
fhv_row_nums as (
    SELECT dispatching_base_num,
        row_number() over (order by dispatching_base_num desc) as rn 
    FROM fhv_base_nums 
),
fhv_vendors as (
    SELECT dispatching_base_num, (rn + 1000) as vendorid
    FROM fhv_row_nums
)

select * from fhv_vendors
