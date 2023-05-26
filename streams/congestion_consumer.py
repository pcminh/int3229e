# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# # os.environ["SPARK_HOME"] = "/usr/hdp/current/spark2-client"
# # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'

# import findspark
# findspark.init()
# findspark.add_packages([
#     "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0",
#     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
# ])

import os 
from dotenv import load_dotenv
load_dotenv()

SOCRATA_APP_TOKEN=os.getenv("SOCRATA_APP_TOKEN")
KAFKA_BOOTSTRAP_SERVER=os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC")
DEST_PSQL_URL=os.getenv("DEST_PSQL_URL")
DEST_PSQL_USER=os.getenv("DEST_PSQL_USER")
DEST_PSQL_PASS=os.getenv("DEST_PSQL_PASS")

from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName("capstone_streaming_traffic_consumer").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

streaming_schema = StructType(
    [
        StructField("segmentid", StringType(), True),
        StructField("street", StringType(), True),
        StructField("_direction", StringType(), True),
        StructField("_fromst", StringType(), True),
        StructField("_tost", StringType(), True),
        StructField("_length", StringType(), True),
        StructField("_strheading", StringType(), True),
        StructField("_comments", StringType(), True),
        StructField("start_lon", DoubleType(), True),
        StructField("_lif_lat", DoubleType(), True),
        StructField("_lit_lon", DoubleType(), True),
        StructField("_lit_lat", DoubleType(), True),
        StructField("_traffic", StringType(), True),
        StructField("_last_updt", TimestampType(), True)
    ]
)

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, lit

def transform_to_f_congestion(df: DataFrame) -> DataFrame:
    df = df.withColumn("value", from_json(col("value"), streaming_schema)).select("value.*")

    df = df.selectExpr(
        "*",
        "date_format(_last_updt + INTERVAL 5 HOURS, 'yyyyMMddHHmm') AS gmtts_str",
        "CAST(format_string('%04d', CAST(segmentid AS int)) AS string) AS segment_padded"
    )
    df = df.selectExpr(
        "CONCAT_WS('-', segment_padded, gmtts_str) AS record_id",
        "CAST(segmentid AS INT) AS segment_id",
        "_last_updt AS timestamp",
        "gmtts_str",
        "CAST(_traffic AS INT) AS speed",
        "CAST(NULL AS INT) as bus_count",
        "CAST(NULL AS INT) as message_count",
        "CAST(date_format(_last_updt, 'yyyyMMdd') AS INT) AS date_key",
        "CAST(date_format(_last_updt, 'HH') AS INT) AS hour_key",
    )

    return df

def append_to_psql(df: DataFrame) -> DataFrame:
    prop = {
        'url': os.getenv("DEST_PSQL_URL"),
        "driver": 'org.postgresql.Driver',
        'user': os.getenv("DEST_PSQL_USER"),
        'password': os.getenv("DEST_PSQL_PASS")
    }
    
    df.write.jdbc(url=prop['url'], table='f_congestion', mode='append', properties=prop)

def save_to_file(df: DataFrame, batch_id: int):
    df \
        .write.format('csv').option('path', 'stream.csv').option('header', 'true').mode('overwrite').save()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, batch_id: append_to_psql(transform_to_f_congestion(df))) \
    .start() \
    .awaitTermination()