select distinct
    camera_id,
    "speed" as type,
    NULL as red_light_intersection,
    address,
    split(address, ' ') as addr_toks, 
    CAST(addr_toks[0] AS INT) as addr_no, 
    concat_ws(' ', filter(addr_toks , (x,i) -> i > 0)) as street_name,
    latitude,
    longitude
from speed_cam