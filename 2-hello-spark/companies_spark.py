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
        .filter(F.col("country") != "United States")                     # WHERE
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

rows, column_names = DataFrameFactory().create_companies_df()
process_industries_outside_us(spark.createDataFrame(rows, column_names))

spark.stop()
sys.exit(0)
