import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path

from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# Load PostgreSQL configuration from environment
pg_host = os.getenv("POSTGRES_CONTAINER_NAME")
pg_dw_db = os.getenv("POSTGRES_DW_DB")
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")

# Configure PostgreSQL JDBC URL
pg_jdbc_url = f"jdbc:postgresql://{pg_host}/{pg_dw_db}"
jdbc_properties = {
    'user': pg_user,
    'password': pg_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
)

spark = (
    pyspark.sql.SparkSession.builder.appName("RuangDataProjectStreaming")
    .master(spark_host)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.18")
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Define the schema using StructType for clarity
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("furniture", StringType()),
    StructField("color", StringType()),
    StructField("price", IntegerType()),
    StructField("ts", LongType())
])

# Parse the JSON data using DataFrame API
parsed_df = stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Calculate average price
avg_price_df = parsed_df.groupBy("furniture").agg(avg("price").alias("avg_price"))

# Define PostgreSQL writer function
def write_to_postgresql(batch_df, batch_id):
    (batch_df.write
        .format("jdbc")
        .option("url", pg_jdbc_url)
        .option("dbtable", "furniture")  # Replace with your table name
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

# Write the result to the console
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgresql) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()
    
query.awaitTermination()
