with d_segment_staging
as (
select distinct
    segment_id,
    UPPER(th.street_heading) as street_heading, 
    UPPER(th.street) as street, 
    CONCAT_WS(' ', UPPER(th.street_heading), UPPER(th.street)) as street_name,
    direction,
    UPPER(from_street) as from_street, 
    UPPER(to_street) as to_street,
    length,
    start_latitude, start_longitude,
    end_latitude, end_longitude
from traffic_hist th
)

select 
    dss.segment_id,
    ds.street_key,
    dss.street_heading,
    dss.street,
    ds.full_street_name as street_name,
    dss.direction,
    dss.from_street,
    dss.to_street,
    dss.length,
    dss.start_latitude,
    dss.start_longitude,
    dss.end_latitude,
    dss.end_longitude
from 
    d_segment_staging dss
inner join 
    (
        select 
            street_key,
            direction,
            street,
            suffix,
            full_street_name,
            row_number() over (partition by direction, street order by suffix) as rn
        from d_street
    ) ds on 
        dss.street_heading = ds.direction
        and dss.street = ds.street
        and ds.rn = 1