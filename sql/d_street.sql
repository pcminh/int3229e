select 
    row_number() over (order by street asc, suffix asc, direction asc) as street_key,
    full_street_name as full_street_name,
    direction as direction,
    street as street,
    suffix_direction as suffix_direction,
    min_address as min_address,
    max_address as max_address
from streets