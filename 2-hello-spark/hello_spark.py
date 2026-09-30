import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame_factory import DataFrameFactory


def process_countries_df():
    rows, column_names = DataFrameFactory().create_countries_df()
    countries = spark.createDataFrame(rows, column_names)
    countries.printSchema()

    print("\n\nSome stats about countries:")
    asian_countries_count = countries.filter(countries.continent == "Asia").count()
    print(f"Countries in Asia: {asian_countries_count}")

    process_countries_group_by_wealth(countries)


def process_countries_group_by_wealth(countries, rich_threshold=20.0, middle_threshold=5.0):
    # GPD is in billions USD and population in millions, so GPD / population = GDP per capita in thousands USD
    countries_with_wealth = (
        countries
        .withColumn("gdp_per_capita_k", F.round(countries.GPD / countries.population, 1))
        .withColumn("wealth",
                    F.when(F.col("gdp_per_capita_k") >= rich_threshold, "rich")
                    .when(F.col("gdp_per_capita_k") >= middle_threshold, "middle")
                    .otherwise("poor"))
    )
    print("\n\nCountries with GDP per capita (thousands USD) and wealth class:")
    countries_with_wealth.orderBy(F.desc("gdp_per_capita_k")).show(truncate=False)

    # GROUP BY: one output row per wealth class
    print("GROUP BY wealth:")
    wealth_stats = (
        countries_with_wealth
        .groupBy("wealth")
        .agg(
            F.count("*").alias("countries"),
            F.sum("population").alias("total_population_m"),
            F.round(F.avg("gdp_per_capita_k"), 1).alias("avg_gdp_per_capita_k"),
        )
        .orderBy(F.desc("avg_gdp_per_capita_k"))
    )
    wealth_stats.show()

    # HAVING: a filter applied AFTER aggregation, on the aggregated values
    print("GROUP BY continent, wealth HAVING count >= 2:")
    (
        countries_with_wealth
        .groupBy("continent", "wealth")
        .agg(F.count("*").alias("countries"))
        .filter(F.col("countries") >= 2)
        .orderBy("continent", "wealth")
        .show()
    )

    # The same two queries in plain SQL
    countries_with_wealth.createOrReplaceTempView("countries")

    print("SQL: GROUP BY wealth")
    spark.sql("""
        SELECT wealth,
               COUNT(*)                         AS countries,
               SUM(population)                  AS total_population_m,
               ROUND(AVG(gdp_per_capita_k), 1)  AS avg_gdp_per_capita_k
        FROM countries
        GROUP BY wealth
        ORDER BY avg_gdp_per_capita_k DESC
    """).show()

    print("SQL: GROUP BY continent, wealth HAVING COUNT(*) >= 2")
    spark.sql("""
        SELECT continent, wealth, COUNT(*) AS countries
        FROM countries
        WHERE continent <> 'Europe/Asia'
        GROUP BY continent, wealth
        HAVING COUNT(*) >= 2
        ORDER BY continent, wealth
    """).show()




def process_greetings_df():
    # 1) The  DataFrame
    df = DataFrameFactory().create_hello_df()
    # greeting = spark.createDataFrame(df[0], df[1])
    greeting = spark.createDataFrame(*df)
    print(
        "A DataFrame is a table: rows and named, typed columns, like a SQL table, an Excel sheet or a pandas DataFrame. "
        + "The difference is that a Spark DataFrame can be split into partitions and processed in parallel, across your CPU "
        + "cores in local mode or across machines on a cluster.")
    print("Check SparkUI at http://localhost:4040")
    greeting.show()
    print("\n\nSchema for GREETINGS looks like that:")
    greeting.printSchema()


def process_spark_word_count():
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

process_greetings_df()
process_spark_word_count()
process_countries_df()

spark.stop()
sys.exit(0)
