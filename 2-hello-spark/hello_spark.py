import os
import sys

from pyspark.sql import SparkSession

# On Windows, make Spark's Python workers use the same interpreter as the driver
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("HelloSpark")
    .master("local[*]")          # local mode, one worker thread per CPU core
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) The literal "Hello World": a one-row DataFrame
greeting = spark.createDataFrame([("Hello, Spark!",)], ["message"])

print("A DataFrame is a table: rows and named, typed columns, like a SQL table, an Excel sheet or a pandas DataFrame. "
      +"The difference is that a Spark DataFrame can be split into partitions and processed in parallel, across your CPU "
      +"cores in local mode or across machines on a cluster.")

greeting.show()

# 2) The classic Spark hello world: word count
lines = [
    "hello spark",
    "hello world",
    "spark is fast and spark is fun",
]
words = spark.sparkContext.parallelize(lines).flatMap(lambda line: line.split())
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for word, count in sorted(word_counts.collect(), key=lambda pair: -pair[1]):
    print(f"{word}: {count}")

spark.stop()
