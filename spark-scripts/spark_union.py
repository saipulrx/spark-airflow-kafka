from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("mapExample").getOrCreate()

data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])

data2 = [(4, "David"), (5, "Eve")]
df2 = spark.createDataFrame(data2, ["id", "name"])

union_df = df.union(df2)
union_df.show()

# stop current spark session
spark.stop()