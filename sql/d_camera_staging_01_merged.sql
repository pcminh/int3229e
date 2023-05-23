with d_cam_speed_upper
as (
select distinct
    camera_id,
    "speed" as type,
    NULL as red_light_intersection,
    UPPER(address) as address,
    latitude,
    longitude
from speed_cam
where camera_id is not null),

d_cam_redlight_upper
as (
select distinct
    camera_id,
    "redlight" as type,
    intersection as red_light_intersection,
    UPPER(address) as address,
    latitude,
    longitude
from redlight_cam
where camera_id is not null),

d_cam_merged
as (
    select * from d_cam_speed_upper
    union 
    select * from d_cam_redlight_upper
)

select 
    *
from d_cam_merged