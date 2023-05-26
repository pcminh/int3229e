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
    )

    select 
        camera_id,
        violation_date_key,
        violation_type,
        violations
    from f_violation_staging_merged
    """)

    return f_violation

def fact_congestion_batch(spark: SparkSession) -> DataFrame:
    f_congestion_batch = spark.sql("""
        with f_congestion_batch_staging
        as (
            select 
                CAST(format_string('%04d', segment_id) AS string) as segment_padded,
                to_timestamp(time, 'MM/dd/yyyy hh:mm:ss a') as timestamp,
                date_format(timestamp + INTERVAL 5 HOURS, 'yyyyMMddHHmm') as gmtts_str,
                CONCAT_WS('-', segment_padded, gmtts_str) AS congestion_key,
                segment_id,
                speed,
                bus_count,
                message_count,
                CAST(date_format(timestamp, 'yyyyMMdd') AS INT) as date_key,
                CAST(date_format(timestamp, 'HH') AS INT) as hour_key
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

def fact_crash(spark: SparkSession, dim_street: DataFrame) -> DataFrame:
    dim_street.createOrReplaceTempView('d_street')
    f_crash = spark.sql("""
        select 
            crash_record_id as crash_record_id,
            rd_no as report_number,
            to_timestamp(crash_date, 'MM/dd/yyyy HH:mm:ss a') as crash_timestamp,
            CAST(date_format(crash_timestamp, 'yyyyMMdd') AS INT) as crash_date_key,
            CAST(date_format(crash_timestamp, 'hh') AS INT) as crash_hour_key,
            posted_speed_limit as posted_speed_limit,
            traffic_control_device as traffic_control_device,
            device_condition as device_condition,
            weather_condition as weather_condition,
            lighting_condition as lighting_condition,
            roadway_surface_cond as roadway_surface_cond,
            road_defect as road_defect,
            first_crash_type as first_crash_type,
            trafficway_type as trafficway_type,
            lane_cnt as lane_cnt,
            report_type as report_type,
            crash_type as crash_type,
            damage as damage,
            street_no as street_no,
            ds.street_key as street_key,
            num_units as num_units,
            most_severe_injury as most_severy_injury,
            injuries_total as injuries_total,
            injuries_fatal as injuries_fatal,
            injuries_incapacitating as injuries_incapacitating,
            injuries_non_incapacitating as injuries_non_incapacitating,
            injuries_reported_not_evident as injuries_reported_not_evident,
            injuries_no_indication as injuries_no_indication,
            injuries_unknown as injuries_unknown,
            latitude as latitude,
            longitude as longitude
        FROM crashes c
        LEFT JOIN d_street ds 
        ON ds.full_street_name LIKE CONCAT('%', UPPER(c.street_name), '%');
    """)
    f_crash = f_crash.drop_duplicates(['crash_record_id'])

    return f_crash