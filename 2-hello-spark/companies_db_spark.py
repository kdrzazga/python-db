import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

POSTGRES_DRIVER_JAR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars", "postgresql-42.7.4.jar")
POSTGRES_DRIVER_URL = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
LOCAL_POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"


class CompaniesDbReport:

    def __init__(self, spark, companies_url, financials_url, user="postgres",
                 year=2024, top_count=3, excluded_country="United States"):
        self.spark = spark
        self.companies_url = companies_url
        self.financials_url = financials_url
        self.user = user
        self.year = year
        self.top_count = top_count
        self.excluded_country = excluded_country

    def read_table(self, url, table):
        print(f"JDBC read: table '{table}' from {url}")
        return self.spark.read.jdbc(
            url, table, properties={"user": self.user, "driver": "org.postgresql.Driver"})

    def read_companies(self):
        companies = self.read_table(self.companies_url, "companies")
        companies.printSchema()
        return companies

    def read_financials(self):
        financials = self.read_table(self.financials_url, "financials")
        financials.printSchema()
        return financials

    def wealthiest_companies(self, companies, financials):
        companies_outside_excluded_country = companies.filter(F.col("country") != self.excluded_country)
        financials_for_year = financials.filter(F.col("year") == self.year)
        billions = F.lit(1_000_000_000)
        return (
            financials_for_year
            .join(F.broadcast(companies_outside_excluded_country), "company_id")
            .orderBy(F.desc("market_cap_usd"))
            .limit(self.top_count)
            .select(
                "name",
                "country",
                "industry",
                F.round(F.col("market_cap_usd") / billions, 1).alias("market_cap_bn"),
                F.round(F.col("revenue_usd") / billions, 1).alias("revenue_bn"),
                F.round(F.col("net_income_usd") / billions, 1).alias("net_income_bn"),
                F.round(100 * F.col("net_income_usd") / F.col("revenue_usd"), 1).alias("margin_pct"),
            )
        )

    def run(self):
        print("\n=== 1. Two separate JDBC sources ===")
        print("Each table is read with its own JDBC connection. Locally both URLs point to the same database,\n"
              "but they could be two different machines - neither database needs to know about the other.\n")
        companies = self.read_companies()
        financials = self.read_financials()
        print("Nothing has been read yet: a JDBC DataFrame only knows the table schema until an action runs.\n")

        print(f"\n=== 2. Top {self.top_count} companies by market cap outside {self.excluded_country} ({self.year}) ===")
        print("Equivalent SQL (impossible as one query if the tables live in two different databases):\n"
              "    SELECT c.name, c.country, c.industry, f.market_cap_usd, ...\n"
              "    FROM financials f\n"
              "    JOIN companies c ON c.company_id = f.company_id\n"
              f"    WHERE c.country <> '{self.excluded_country}' AND f.year = {self.year}\n"
              "    ORDER BY f.market_cap_usd DESC\n"
              f"    LIMIT {self.top_count};\n")
        result = self.wealthiest_companies(companies, financials)

        print("\n=== 3. Query plan ===")
        print("Look for:\n"
              "  - PushedFilters: the WHERE conditions Spark sent to PostgreSQL, so fewer rows travel over the network\n"
              "  - BroadcastHashJoin: the join runs inside Spark; the small 'companies' table is copied to every executor\n"
              "  - TakeOrderedAndProject: ORDER BY + LIMIT done as a top-N, without sorting everything\n")
        result.explain()

        print("\n=== 4. Result (amounts in billions of USD) ===")
        print("show() is the action: only now Spark opens the JDBC connections and reads the data.\n")
        result.show(truncate=False)

        print("Notes for big tables:\n"
              "  - by default each JDBC table is read through ONE connection, i.e. one task\n"
              "  - read.jdbc(url, table, column='financial_id', lowerBound=..., upperBound=..., numPartitions=8)\n"
              "    splits the read into 8 range queries running in parallel\n"
              "  - in production, tables are usually copied to Parquet/Delta on S3/HDFS first,\n"
              "    so Spark does not hammer the operational databases")


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if not os.path.isfile(POSTGRES_DRIVER_JAR):
    print(f"PostgreSQL JDBC driver not found: {POSTGRES_DRIVER_JAR}\n"
          f"Download it from {POSTGRES_DRIVER_URL}")
    sys.exit(1)

print(f"Starting Spark with the PostgreSQL JDBC driver on the driver classpath: {POSTGRES_DRIVER_JAR}\n"
      "This works in local mode only, where the driver and executors share one JVM.\n"
      "On a cluster the jar must reach every executor: spark-submit --jars or spark.jars.packages.")
spark = (
    SparkSession.builder
    .appName("CompaniesDbSpark")
    .master("local[*]")
    .config("spark.driver.extraClassPath", POSTGRES_DRIVER_JAR)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Password is not in the code: the PostgreSQL JDBC driver reads %APPDATA%\\postgresql\\pgpass.conf (setup-pgpass.bat).")

CompaniesDbReport(spark, companies_url=LOCAL_POSTGRES_URL, financials_url=LOCAL_POSTGRES_URL).run()

spark.stop()
sys.exit(0)
