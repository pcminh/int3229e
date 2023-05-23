with hour_seq as (
    select 
        explode(
            sequence( 0, 23, 1 )
        ) as hour_key
)

SELECT
    hour_key,
    CASE
        WHEN hour_key = 0 THEN 12
        WHEN hour_key <= 12 THEN hour_key
        ELSE hour_key - 12
    END AS meridiem_hour,
    CASE
        WHEN hour_key < 12 THEN "AM"
        ELSE "PM"
    END AS meridiem_suffix,
    CASE
        WHEN (hour_key >= 6 AND hour_key <= 17) THEN "day"
        ELSE "night"
    END AS day_night
FROM
    hour_seq