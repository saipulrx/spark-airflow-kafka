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

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
)

spark = (
    pyspark.sql.SparkSession.builder.appName("RuangDataProjectStreaming")
    .master(spark_host)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
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

schema_sql = "order_id STRING, customer_id INT, furniture STRING, color STRING, price INT, ts BIGINT"

# First register the DataFrame as a temporary view
stream_df.createOrReplaceTempView("kafka_source")

#parsed_df = stream_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
avg_price_df = spark.sql(
    f"""
    SELECT 
    parsed_data.furniture,
        AVG(parsed_data.price) AS avg_price
    FROM (
        SELECT 
            from_json(CAST(value AS STRING), '{schema_sql}') AS data
        FROM kafka_source
    )
    LATERAL VIEW inline(array(data)) parsed_data
    GROUP BY parsed_data.furniture
    """
)

# Write the result to the console
query = avg_price_df.writeStream.outputMode("update").format("console").trigger(processingTime="5 seconds").start()

query.awaitTermination()
