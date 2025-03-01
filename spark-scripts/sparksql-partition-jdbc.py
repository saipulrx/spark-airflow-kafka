from pyspark.sql import SparkSession
import pyspark

postgres_host = 'localhost'
postgres_dw_db = 'demo_intro_sql'
postgres_user = 'postgres'
postgres_password = '12345'

#spark = SparkSession.builder.appName("Example Partition table from db").getOrCreate()
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Example Partition table from db')
        .setMaster('local')
        .set("spark.jars", "jars/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# JDBC connection
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Load data (replace with your dataset)
orders_df = spark.read.jdbc(
    jdbc_url,
    'public.orders',
    properties=jdbc_properties
)
orders_df.show()

# Register the DataFrame as a SQL temporary view
orders_df.createOrReplaceTempView("orders_df")

# filter data by location China
filter_df = spark.sql("""
                   SELECT *
                  FROM orders_df
                  where ship_country = 'USA'
                  """)
filter_df.show()

# Write to Parquet with partitioning by year and month
filter_df.write.partitionBy("order_date") \
  .mode("overwrite") \
  .jdbc(
        jdbc_url,
        'public.orders_usa',
        properties=jdbc_properties
    )

# Read partitioned data
partitioned_df = spark.read.jdbc(
        jdbc_url,
        'public.orders_usa',
        properties=jdbc_properties
    ).show()

# Filter on partition columns (triggers partition pruning)
df_filtered = partitioned_df.filter((partitioned_df.order_date == "2023-01-01"))

df_filtered.show()

#clone spark session
spark.stop()