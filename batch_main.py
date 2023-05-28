# %%
# import os 
# os.environ["SPARK_HOME"] = ""
# # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.10 pyspark-shell'

# import findspark
# findspark.init()
# findspark.add_packages(
#     ['org.postgresql:postgresql:42.2.10']
# )
# findspark.add_jars(
#     [
#         'postgresql-42.6.0.jar'
#     ]
# )


# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col, avg

spark = SparkSession.builder\
    .appName("int3229e_batch")\
    .getOrCreate()

# %%
spark.sql("SHOW DATABASES").show()

# %%
from batch.rw import read_table_to_view
read_table_to_view(spark)
spark.sql("SHOW TABLES").show()


# %%
from batch.dimensions import dim_street, dim_camera_stage, dim_camera_map_street, dim_segment, dim_date, dim_hour
from batch.facts import fact_violation, fact_crash, fact_congestion_batch

# %%
d_street = dim_street(spark)

# %%
d_camera = dim_camera_stage(spark)
d_camera = dim_camera_map_street(spark, d_camera, d_street)

# %%
d_segment = dim_segment(spark, d_street)

# %%
d_date = dim_date(spark)
d_hour = dim_hour(spark)

# %%
f_violation = fact_violation(spark)

# %%
f_crash = fact_crash(spark, d_street)

# %%
f_congestion = fact_congestion_batch(spark)

# %%

from batch.rw import write_to_psql

write_to_psql(d_street, 'd_street', 'overwrite')
write_to_psql(d_camera, 'd_camera', 'overwrite')
write_to_psql(d_segment, 'd_segment', 'overwrite')
write_to_psql(d_date, 'd_date', 'overwrite')
write_to_psql(d_hour, 'd_hour', 'overwrite')

write_to_psql(f_violation, 'f_violation', 'overwrite')
write_to_psql(f_crash, 'f_crash', 'overwrite')
write_to_psql(f_congestion, 'f_crash', 'overwrite')
