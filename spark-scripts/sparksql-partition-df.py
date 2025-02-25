from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionExample").getOrCreate()

# Load data (replace with your dataset)
data = [("2023", "01", "ProductA", 100),
        ("2023", "01", "ProductB", 200),
        ("2023", "02", "ProductA", 150)]
columns = ["year", "month", "product", "revenue"]
df = spark.createDataFrame(data, columns)

# Write to Parquet with partitioning by year and month
df.write.partitionBy("year", "month") \
  .mode("overwrite") \
  .parquet("sales_partitioned.parquet")

# Read partitioned data
partitioned_df = spark.read.parquet("sales_partitioned.parquet")

# Filter on partition columns (triggers partition pruning)
df_filtered = partitioned_df.filter((partitioned_df.year == "2023") & (partitioned_df.month == "01"))

df_filtered.show()