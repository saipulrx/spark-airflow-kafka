from pyspark.sql import SparkSession

spark = SparkSession \
 .builder \
 .appName("Data Frame Example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()