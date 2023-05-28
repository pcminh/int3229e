from pyspark.sql import SparkSession, DataFrame
from .schemas import redlight_cam_schema, speed_cam_schema, streets_schema, traffic_hist_schema
import os

# dataset root path: đường dẫn hdfs/fs đến thư mục chứa các tập dữ liệu, mỗi tập dữ liệu chứa trong thư mục với tên tương ứng (crashes, redlight_cam, speed_cam, streets, traffic_hist)
def read_table_to_view(spark: SparkSession, dataset_root_path: str) -> dict:
    tables = [
        'crashes', 
        'redlight_cam', 
        'speed_cam', 
        'streets', 
        'traffic_hist'
    ]

    paths = {
        table: os.path.join(dataset_root_path, f'{table}/{table}.csv')
        for table in tables
    } 

    schemas = {
        # 'crashes'
        'redlight_cam': redlight_cam_schema,
        'speed_cam': speed_cam_schema,
        'streets': streets_schema,
        'traffic_hist': traffic_hist_schema
    }

    ret = {}
    
    for table in tables:
        read_obj = spark.read.format("csv").option("header", "true")

        if table in schemas:
            read_obj = read_obj.schema(schemas[table])
        else:
            read_obj = read_obj.option("inferSchema", "true")
 
        df = read_obj.load(paths[table])
        df.createOrReplaceTempView(table)
        
        ret[table] = df 
    
    return ret

def write_to_psql(df: DataFrame, table_name: str, write_mode: str) -> DataFrame:
    prop = {
        'url': f'jdbc:postgresql://{os.getenv("SINK_PSQL_HOST")}:{os.getenv("SINK_PSQL_PORT")}/{os.getenv("SINK_PSQL_DB")}',
        "driver": 'org.postgresql.Driver',
        'user': os.getenv("SINK_PSQL_USER"),
        'password': os.getenv("SINK_PSQL_PASS")
    }
    
    df.write.jdbc(url=prop['url'], table=table_name, mode=write_mode, properties=prop)
