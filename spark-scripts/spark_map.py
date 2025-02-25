from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("mapExample").getOrCreate()

data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])

# Using map equivalent in DataFrame API (withColumn)
df_new = df.withColumn("id_plus_one", F.col("id") + 1)
df_new.show()

# stop current spark session
spark.stop()