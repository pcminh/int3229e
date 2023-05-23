select 
    camera_id,
    violation_date_mmddyyyy,
    to_date(violation_date_mmddyyyy, "MM/dd/yyyy") as violation_date,
    date_format(violation_date, "yyyyMMdd") as violation_date_key,
    "redlight" as violation_type,
    violations
from redlight_cam