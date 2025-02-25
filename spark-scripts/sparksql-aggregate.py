from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

df = spark.read.format("csv").option("header", "true").load("/resources/data/online-retail-dataset.csv")

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("retail")

sqlDF = spark.sql("""
                   SELECT country, sum(quantity * unitprice) as total_price
                  FROM retail
                  group by country
                  order by total_price desc
                  """)
sqlDF.show()

# stop current spark session
spark.stop()