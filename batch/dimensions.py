from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, when

def dim_street(spark: SparkSession) -> DataFrame:
    d_street = spark.sql("""
        select 
            row_number() over (order by street asc, suffix asc, direction asc) as street_key,
            full_street_name as full_street_name,
            direction as direction,
            street as street,
            suffix as suffix,
            suffix_direction as suffix_direction,
            min_address as min_address,
            max_address as max_address
        from streets
    """)

    return d_street

def dim_camera_stage(spark: SparkSession) -> DataFrame:
    dcam = spark.sql("""
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
    """)


    dcam = dcam.withColumn("address",
                        when(dcam.address.endswith("STREET"), regexp_replace(dcam.address, "STREET", "ST")) \
                        .when(dcam.address.endswith("STREE"), regexp_replace(dcam.address, "STREE", "ST")) \
                        .when(dcam.address.endswith("AVENUE"), regexp_replace(dcam.address, "AVENUE", "AVE")) \
                        .when(dcam.address.endswith("AVENU"), regexp_replace(dcam.address, "AVENU", "AVE")) \
                        .when(dcam.address.endswith("AVEN"), regexp_replace(dcam.address, "AVEN", "AVE")) \
                        .when(dcam.address.endswith("PARKWAY"), regexp_replace(dcam.address, "PARKWAY", "PKWY")) \
                        .when(dcam.address.endswith("PARKWA"), regexp_replace(dcam.address, "PARKWA", "PKWY")) \
                        .when(dcam.address.endswith("ROAD"), regexp_replace(dcam.address, "ROAD", "RD")) \
                        .when(dcam.address.endswith("ROA"), regexp_replace(dcam.address, "ROA", "RD")) \
                        .when(dcam.address.endswith("BOULEVARD"), regexp_replace(dcam.address, "BOULEVARD", "BLVD")) \
                        .when(dcam.address.endswith("BOUL"), regexp_replace(dcam.address, "BOUL", "BLVD")) \
                        .when(dcam.address.endswith("DRIVE"), regexp_replace(dcam.address, "DRIVE", "DR")) \
                        .when(dcam.address.endswith("DRIV"), regexp_replace(dcam.address, "DRIV", "DR")) \
                        .when(dcam.address.endswith("MARTIN LUTHER KING"), regexp_replace(dcam.address, "MARTIN LUTHER KING", "DR MARTIN LUTHER KING")) \
                        .when(dcam.address.endswith("DR MARTIN L KING"), regexp_replace(dcam.address, "DR MARTIN L KING", "DR MARTIN LUTHER KING")) \
                        .when(dcam.address.endswith("MARTIN L KING"), regexp_replace(dcam.address, "MARTIN L KING", "DR MARTIN LUTHER KING")) \
                        .otherwise(dcam.address)
                        )

    dcam.createOrReplaceTempView("d_camera_staging")

    dcam = spark.sql("""
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
    """)

    return dcam

def dim_camera_map_street(spark: SparkSession, dim_camera_staging: DataFrame, dim_street: DataFrame) -> DataFrame:
    dim_street.createOrReplaceTempView("d_street")
    dim_camera_staging.createOrReplaceTempView("d_camera_staging")

    d_camera = spark.sql("""
        SELECT 
            camera_id, 
            type, 
            red_light_intersection, 
            address, 
            street_key, 
            latitude, 
            longitude
        FROM 
            d_camera_staging dcs
        LEFT JOIN
            d_streets ds
        ON ds.full_street_name = CONCAT(dcs.street_name, '%')
    """)

    return d_camera

def dim_segment(spark: SparkSession) -> DataFrame:
    d_segment = spark.sql("""
        with d_segment_staging
        as (
        select distinct
            segment_id,
            UPPER(th.street_heading) as street_heading, 
            UPPER(th.street) as street, 
            CONCAT_WS(' ', UPPER(th.street_heading), UPPER(th.street)) as street_name,
            direction,
            UPPER(from_street) as from_street, 
            UPPER(to_street) as to_street,
            length,
            start_latitude, start_longitude,
            end_latitude, end_longitude
        from traffic_hist th
        )

        select 
            dss.segment_id,
            ds.street_key,
            dss.street_heading,
            dss.street,
            ds.full_street_name as street_name,
            dss.direction,
            dss.from_street,
            dss.to_street,
            dss.length,
            dss.start_latitude,
            dss.start_longitude,
            dss.end_latitude,
            dss.end_longitude
        from 
            d_segment_staging dss
        inner join 
            (
                select 
                    street_key,
                    direction,
                    street,
                    suffix,
                    full_street_name,
                    row_number() over (partition by direction, street order by suffix) as rn
                from d_street
            ) ds on 
                dss.street_heading = ds.direction
                and dss.street = ds.street
                and ds.rn = 1
    """)

    return d_segment

def dim_date(spark: SparkSession) -> DataFrame:
    d_date = spark.sql("""
        with raw_dates as (
            select 
                explode(
                    sequence( to_date('2000-01-01'), to_date('2099-12-31'), interval 1 day )
                ) as calendar_date
        )

        select 
            CAST(date_format(calendar_date, "yyyyMMdd") AS INT) as date_key,
            calendar_date as date_alternate_key,
            year(calendar_date) AS calendar_year,
            date_format(calendar_date, 'MMMM') as calendar_month,
            month(calendar_date) as month_of_year,
            date_format(calendar_date, 'EEEE') as calendar_day,
            dayofweek(calendar_date) AS day_of_week_sun,
            weekday(calendar_date) + 1 as day_of_week_mon,
            case
                when weekday(calendar_date) < 5 then 'Y'
                else 'N'
            end as is_week_day,
            dayofmonth(calendar_date) as day_of_month,
            case
                when calendar_date = last_day(calendar_date) then 'Y'
                else 'N'
            end as is_last_day_of_month,
            dayofyear(calendar_date) as day_of_year,
            weekofyear(calendar_date) as week_of_year_iso,
            quarter(calendar_date) as quarter_of_year
        from 
            raw_dates
    """)

    return d_date

def dim_hour(spark: SparkSession) -> DataFrame:
    d_hour = spark.sql("""
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
    """)

    return d_hour