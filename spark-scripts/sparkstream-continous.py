from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("MicroBatchExample").getOrCreate()

# Read streaming data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input_topic") \
    .load()

# Convert Kafka value column (binary) to string
lines = df.selectExpr("CAST(value AS STRING)")

# Word count
words = lines.select(explode(split(lines.value, " ")).alias("word"))
word_counts = words.groupBy("word").count()

# Write output to console in Continuous processing mode
query = word_counts.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(continuous="1 second") \
    .start()

query.awaitTermination()