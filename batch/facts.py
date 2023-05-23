from pyspark.sql import SparkSession, DataFrame

def fact_violation(spark: SparkSession) -> DataFrame:
    f_violation = spark.sql("""
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
    """)

    return f_violation

def f_congestion_batch(spark: SparkSession) -> DataFrame:
    f_congestion_batch = spark.sql("""
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
            congestion_key as record_id,
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
    """)

    return f_congestion_batch