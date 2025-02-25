from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="DStreamExample")
ssc = StreamingContext(sc, 5)  # Batch interval of 5 seconds

# Read data from a socket
lines = ssc.socketTextStream("localhost", 9999)

# Word count transformation
word_counts = lines.flatMap(lambda line: line.split(" ")) \
                   .map(lambda word: (word, 1)) \
                   .reduceByKey(lambda a, b: a + b)

word_counts.pprint()

ssc.start()
ssc.awaitTermination()