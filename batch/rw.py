from pyspark.sql import SparkSession
from schemas import redlight_cam_schema, speed_cam_schema, streets_schema, traffic_hist_schema


def read_table_to_view(spark: SparkSession):
    tables = [
        'crashes', 
        'redlight_cam', 
        'speed_cam', 
        'streets', 
        'traffic_hist'
    ]

    paths = {
        table: f'./data/raw/traffic/{table}/{table}.csv'
        for table in tables
    } 

    schemas = {
        # 'crashes'
        'redlight_cam': redlight_cam_schema,
        'speed_cam': speed_cam_schema,
        'streets': streets_schema,
        'traffic_hist': traffic_hist_schema
    }

    for table in tables:
        read_obj = spark.read.format("csv").option("header", "true")

        if table in schemas:
            read_obj = read_obj.schema(schemas[table])
        else:
            read_obj = read_obj.option("inferSchema", "true")
 
        df = read_obj.load(paths[table])
        df.createOrReplaceTempView(table)