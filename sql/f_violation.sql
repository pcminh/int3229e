with f_violation_redlight_staging 
as (
    select 
        camera_id,
        violation_date_mmddyyyy,
        to_date(violation_date_mmddyyyy, "MM/dd/yyyy") as violation_date,
        date_format(violation_date, "yyyyMMdd") as violation_date_key,
        "redlight" as violation_type,
        violations
    from redlight_cam
),

f_violation_speed_staging 
as (
    select 
        camera_id,
        violation_date_mmddyyyy,
        to_date(violation_date_mmddyyyy, "MM/dd/yyyy") as violation_date,
        date_format(violation_date, "yyyyMMdd") as violation_date_key,
        "speed" as violation_type,
        violations
    from speed_cam
),

f_violation_staging_merged
as (
    select * from f_violation_redlight_staging
    union
    select * from f_violation_speed_staging 
),

select 
    camera_id,
    violation_date_key,
    violation_type,
    violations
from f_violation_staging_merged
