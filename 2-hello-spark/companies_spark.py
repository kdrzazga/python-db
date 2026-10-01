import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame_factory import DataFrameFactory


def process_industries_outside_us(companies):
    #  sql/companies.sql:
    #   SELECT industry, COUNT(*) AS companies, STRING_AGG(name, ', ') AS names
    #   FROM companies
    #   WHERE country <> 'United States'
    #   GROUP BY industry
    #   HAVING COUNT(*) >= 2;
    print("Industries with at least two companies based outside the US:")
    (
        companies
        .filter(F.col("country") != "United States")                      # WHERE
        .groupBy("industry")                                              # GROUP BY
        .agg(
            F.count("*").alias("companies"),
            F.concat_ws(", ", F.collect_list("name")).alias("names"),    # STRING_AGG
        )
        .filter(F.col("companies") >= 2)                                  # HAVING
        .show(truncate=False)
    )


# On Windows, make Spark's Python workers use the same interpreter as the driver
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("CompaniesSpark")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

info = '''What LOCAL mode hides:
1. Where the data comes from. Your data is a Python list passed to createDataFrame, so it starts on the driver. 
On a cluster the data would normally come from shared storage the executors read directly, such as HDFS, S3 or database tables: spark.read.parquet("s3://...").
2. The shuffle. groupBy("industry") needs every row of one industry in the same place. 
On a cluster, rows move between machines over the network to get there, and that is often the slowest part of a job. 
Locally they only move between threads.
3 .show() and .collect(). Both bring results back to the driver. That's fine for a few rows, but calling .collect() on a billion rows can run the driver out of memory.
4. Failures. On a cluster, a machine can fail partway through a job. Spark re-runs the lost tasks from the plan, 
which is one reason DataFrames can't be changed and are computed only when needed.'''

print(info)

rows, column_names = DataFrameFactory().create_companies_df()
data_frame = spark.createDataFrame(rows, column_names) # in real life scenario -> spark.read.parquet("s3://bucket/companies/")
process_industries_outside_us(data_frame)

spark.stop()
sys.exit(0)
