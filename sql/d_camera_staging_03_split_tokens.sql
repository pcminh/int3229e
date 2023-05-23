SELECT
    camera_id,
    type,
    red_light_intersection,
    address,
    split(UPPER(address), ' ') as addr_toks, 
    CAST(addr_toks[0] AS INT) as addr_no, 
    concat_ws(' ', filter(addr_toks , (x,i) -> i > 0)) as street_name,
    latitude,
    longitude
FROM d_camera_staging