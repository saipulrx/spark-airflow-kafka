from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example Partition table from csv file").getOrCreate()

# Load data (replace with your dataset)
df_owid_covid = spark.read.format("csv").option("header","true")\
    .load("data/owid-covid-data.csv")

# Register the DataFrame as a SQL temporary view
df_owid_covid.createOrReplaceTempView("owid_covid_2")

# filter data by location China
filter_df = spark.sql("""
                   SELECT *
                  FROM owid_covid_2
                  where location = 'China'
                  """)
filter_df.show()

# Write to Parquet with partitioning by year and month
filter_df.write.partitionBy("date") \
  .mode("overwrite") \
  .parquet("owid_covid_partitioned.parquet")

# Read partitioned data
partitioned_df = spark.read.parquet("owid_covid_partitioned.parquet")

# Filter on partition columns (triggers partition pruning)
df_filtered = partitioned_df.filter((partitioned_df.date == "2023-01-01"))

df_filtered.show()