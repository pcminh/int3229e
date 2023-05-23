with f_congestion_batch_staging
as (
    select 
        CAST(format_string('%04d', segment_id) AS string) as segment_padded,
        to_timestamp(time, 'MM/dd/yyyy HH:mm:ss a') as timestamp,
        date_format(timestamp + INTERVAL 5 HOURS, 'yyyyMMddhhmm') as gmtts_str,
        CONCAT_WS('-', segment_padded, gmtts_str) AS congestion_key,
        segment_id,
        speed,
        bus_count,
        message_count,
        CAST(date_format(timestamp, 'yyyyMMdd') AS INT) as date_key,
        CAST(date_format(timestamp, 'hh') AS INT) as hour_key
    from traffic_hist
)

select 
    congestion_key as congestion_record_id,
    segment_id, 
    timestamp,
    gmtts_str,
    speed,
    bus_count,
    message_count,
    date_key,
    hour_key
from f_congestion_batch_staging
where speed > -1