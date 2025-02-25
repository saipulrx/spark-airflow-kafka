from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("mapExample").getOrCreate()

data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])

#use filter transformation
filtered_df = df.filter(F.col("id") < 3)
filtered_df.show()

# stop current spark session
spark.stop()