from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampNTZType

redlight_cam_schema = StructType(
    [
        StructField("intersection", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("address", StringType(), True),
        StructField("violation_date_mmddyyyy", StringType(), True),
        StructField("violations", IntegerType(), True),
        StructField("x_coordinate", DoubleType(), True),
        StructField("y_coordinate", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location", StringType(), True)
    ]
)

speed_cam_schema = StructType(
    [
        StructField("address", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("violation_date_mmddyyyy", StringType(), True),
        StructField("violations", IntegerType(), True),
        StructField("x_coordinate", DoubleType(), True),
        StructField("y_coordinate", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location", StringType(), True)
    ]
)

streets_schema = StructType(
    [
        StructField("full_street_name", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("street", StringType(), True),
        StructField("suffix", StringType(), True),
        StructField("suffix_direction", StringType(), True),
        StructField("min_address", IntegerType(), True),
        StructField("max_address", IntegerType(), True),
        
    ]
)

traffic_hist_schema = StructType(
    [
        StructField("time", StringType(), True),
        StructField("segment_id", IntegerType(), True),
        StructField("speed", IntegerType(), True),
        StructField("street", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("from_street", StringType(), True),
        StructField("to_street", StringType(), True),
        StructField("length", IntegerType(), True),
        StructField("street_heading", StringType(), True),
        StructField("comments", StringType(), True),
        StructField("bus_count", IntegerType(), True),
        StructField("message_count", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("record_id", StringType(), True),
        StructField("start_latitude", DoubleType(), True),
        StructField("start_longitude", DoubleType(), True),
        StructField("end_latitude", DoubleType(), True),
        StructField("end_longitude", DoubleType(), True),
        StructField("start_location", StringType(), True),
        StructField("end_location", StringType(), True),
        
    ]
)