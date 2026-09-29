import duckdb
import psycopg


class PersonTaxEtl:
    def __init__(self, postgres_dsn: str, parquet_path: str | None = None):
        self.postgres_dsn = postgres_dsn
        self.parquet_path = parquet_path
        self.write_info()

    def extract(self, connection):
        persons = connection.execute("SELECT id, name, last_name, tax_id FROM person").fetchall()
        taxes = connection.execute("SELECT id, income FROM tax").fetchall()
        return persons, taxes

    def transform(self, persons, taxes):
        workspace = duckdb.connect(":memory:")
        workspace.execute("CREATE TABLE person (id INT, name TEXT, last_name TEXT, tax_id INT)")
        workspace.execute("CREATE TABLE tax (id INT, income DECIMAL(12,2))")
        workspace.executemany("INSERT INTO person VALUES (?, ?, ?, ?)", persons)
        workspace.executemany("INSERT INTO tax VALUES (?, ?)", taxes)
        workspace.execute("""
            CREATE TABLE person_tax AS
            SELECT p.id AS person_id,
                   p.name || ' ' || p.last_name AS full_name,
                   t.income,
                   CASE WHEN t.income < 20000  THEN 'low'
                        WHEN t.income < 100000 THEN 'mid'
                        ELSE 'high' END AS income_bracket
            FROM person p JOIN tax t ON t.id = p.tax_id
        """)
        return workspace

    def load(self, connection, workspace):
        rows = workspace.execute("SELECT person_id, full_name, income, income_bracket FROM person_tax").fetchall()
        connection.execute("CREATE SCHEMA IF NOT EXISTS report")
        connection.execute("DROP TABLE IF EXISTS report.person_tax")
        connection.execute("""
            CREATE TABLE report.person_tax (
                person_id int PRIMARY KEY, full_name text,
                income numeric(12,2), income_bracket text)
        """)
        with connection.cursor() as cursor:
            cursor.executemany("INSERT INTO report.person_tax VALUES (%s, %s, %s, %s)", rows)

        if self.parquet_path:
            escaped_path = self.parquet_path.replace("'", "''")
            workspace.execute(f"COPY person_tax TO '{escaped_path}' (FORMAT PARQUET)")

    def run(self):
        with psycopg.connect(self.postgres_dsn) as connection:
            persons, taxes = self.extract(connection)
            workspace = self.transform(persons, taxes)
            self.load(connection, workspace)
            workspace.close()

    def write_info(self):
        lines = (3*"\n", "This application does the ETL:", "1. EXTRACT - It reads data from PostgreSQL DB"
                 , "2. TRANSFORM - it re-works data and stores them in in-memory DB DUCK"
                 , "3. LOAD - It creates a report based on those data, and stores them back in PostgreSQL, but in REPORT"
                   + " table.")

        for line in lines:
            print(line)


if __name__ == "__main__":
    PersonTaxEtl("host=localhost port=5432 dbname=postgres user=postgres").run()
