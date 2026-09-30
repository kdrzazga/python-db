"""Multi-format ETL: CSV + JSON Lines + YAML -> DuckDB (staging, cleaning) -> PostgreSQL.

1. EXTRACT   - DuckDB reads the CSV and JSON Lines files natively. DuckDB has no YAML reader,
               so the YAML stream is parsed document by document and staged as JSON Lines first.
2. TRANSFORM - every source is mapped onto one common shape (its own date formats, flags, decimal
               separators, country spellings), cleaned, validated, and de-duplicated by customer_id.
3. LOAD      - the result is bulk-copied into the PostgreSQL schema multi_format with COPY, in one transaction.
The DuckDB workspace is a file (data/work/staging.duckdb), so it can be inspected after the run.
"""
import argparse
import json
import time
from pathlib import Path

import duckdb
import psycopg
import yaml

from reference_data import COUNTRIES, CRM_FILE, LEGACY_FILE, MANIFEST_FILE, WEBSHOP_FILE

try:
    from yaml import CSafeLoader as YamlLoader  # libyaml-based, many times faster
except ImportError:
    from yaml import SafeLoader as YamlLoader

WEBSHOP_COLUMNS = {
    "id": "VARCHAR", "firstName": "VARCHAR", "lastName": "VARCHAR", "email": "VARCHAR", "phone": "VARCHAR",
    "birthDate": "VARCHAR",
    "address": "STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR, countryCode VARCHAR)",
    "createdAt": "VARCHAR", "active": "VARCHAR", "ltv": "VARCHAR",
}
LEGACY_COLUMNS = {
    name: "VARCHAR" for name in ("member_id", "name", "email", "phone", "born", "street", "city", "postcode",
                                 "country", "joined", "status", "value")
}
CUSTOMER_COLUMNS = ("customer_id", "first_name", "last_name", "email", "phone", "birth_date", "street", "city",
                    "postal_code", "country_code", "registered_at", "is_active", "lifetime_value", "sources")
REJECTED_COLUMNS = ("source", "source_row", "customer_id", "reason")
COUNTRY_COLUMNS = ("code", "name")


def sql_string(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def sql_path(path: Path) -> str:
    return sql_string(path.resolve().as_posix())


def sql_columns_struct(columns: dict) -> str:
    return "{" + ", ".join(f"{sql_string(name)}: {sql_string(type_name)}" for name, type_name in columns.items()) + "}"


class MultiFormatEtl:
    def __init__(self, postgres_dsn: str, data_dir: Path, copy_chunk_bytes: int = 1024 * 1024):
        self.postgres_dsn = postgres_dsn
        self.data_dir = Path(data_dir)
        self.work_dir = self.data_dir / "work"
        self.copy_chunk_bytes = copy_chunk_bytes

    # ---------- extract ----------

    def extract(self, workspace):
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.extract_crm(workspace)
        self.extract_webshop(workspace)
        self.extract_legacy(workspace)

    def extract_crm(self, workspace):
        workspace.execute(f"""
            CREATE OR REPLACE TABLE raw_crm AS
            SELECT row_number() OVER () AS source_row, *
            FROM read_csv({sql_path(self.data_dir / CRM_FILE)},
                          delim = ';', quote = '"', header = true, all_varchar = true)
        """)

    def extract_webshop(self, workspace):
        workspace.execute(f"""
            CREATE OR REPLACE TABLE raw_webshop AS
            SELECT row_number() OVER () AS source_row, *
            FROM read_json({sql_path(self.data_dir / WEBSHOP_FILE)},
                           format = 'newline_delimited', columns = {sql_columns_struct(WEBSHOP_COLUMNS)})
        """)

    def extract_legacy(self, workspace):
        staged_path = self.work_dir / "legacy_members.jsonl"
        with (self.data_dir / LEGACY_FILE).open(encoding="utf-8") as yaml_file, \
                staged_path.open("w", encoding="utf-8", newline="\n") as staged_file:
            for document in yaml.load_all(yaml_file, Loader=YamlLoader):
                for member in document or ():
                    staged_file.write(json.dumps(self.flatten_member(member), ensure_ascii=False, default=str) + "\n")

        workspace.execute(f"""
            CREATE OR REPLACE TABLE raw_legacy AS
            SELECT row_number() OVER () AS source_row, *
            FROM read_json({sql_path(staged_path)},
                           format = 'newline_delimited', columns = {sql_columns_struct(LEGACY_COLUMNS)})
        """)

    @staticmethod
    def flatten_member(member: dict) -> dict:
        contact = member.get("contact") or {}
        location = member.get("location") or {}
        return {
            "member_id": member.get("member_id"), "name": member.get("name"),
            "email": contact.get("email"), "phone": contact.get("phone"), "born": member.get("born"),
            "street": location.get("street"), "city": location.get("city"),
            "postcode": location.get("postcode"), "country": location.get("country"),
            "joined": member.get("joined"), "status": member.get("status"), "value": member.get("value"),
        }

    # ---------- transform ----------

    def transform(self, workspace):
        self.create_country_tables(workspace)
        self.parse_sources(workspace)
        self.clean_and_validate(workspace)
        self.deduplicate(workspace)

    @staticmethod
    def create_country_tables(workspace):
        workspace.execute("CREATE OR REPLACE TABLE country (code VARCHAR PRIMARY KEY, name VARCHAR)")
        workspace.executemany("INSERT INTO country VALUES (?, ?)",
                              [(country.code, country.name) for country in COUNTRIES])

        spellings = {spelling.strip().lower(): country.code
                     for country in COUNTRIES for spelling in country.all_names()}
        workspace.execute("CREATE OR REPLACE TABLE country_spelling (spelling VARCHAR PRIMARY KEY, code VARCHAR)")
        workspace.executemany("INSERT INTO country_spelling VALUES (?, ?)", list(spellings.items()))

    @staticmethod
    def parse_sources(workspace):
        """Maps each source's own conventions onto one common, typed shape."""
        workspace.execute("""
            CREATE OR REPLACE TABLE customer_parsed AS
            SELECT 'crm' AS source, 1 AS source_priority, source_row,
                   customer_id AS customer_id_text, first_name, last_name, email, phone,
                   try_strptime(birth_date, '%d.%m.%Y')::DATE AS birth_date,
                   street, city, postal_code, country AS country_text,
                   try_strptime(registered_at, '%Y-%m-%d %H:%M:%S') AS registered_at,
                   CASE upper(trim(active)) WHEN 'Y' THEN true WHEN 'N' THEN false END AS is_active,
                   TRY_CAST(replace(trim(lifetime_value), ',', '.') AS DECIMAL(12, 2)) AS lifetime_value
            FROM raw_crm
            UNION ALL
            SELECT 'webshop', 2, source_row,
                   id, "firstName", "lastName", email, phone,
                   try_strptime("birthDate", '%Y-%m-%d')::DATE,
                   address.street, address.city, address.zip, address."countryCode",
                   try_strptime("createdAt", '%Y-%m-%dT%H:%M:%SZ'),
                   TRY_CAST(active AS BOOLEAN),
                   TRY_CAST(trim(ltv) AS DECIMAL(12, 2))
            FROM raw_webshop
            UNION ALL
            SELECT 'legacy', 3, source_row,
                   member_id, split_part(name, ',', 2), split_part(name, ',', 1), email, phone,
                   try_strptime(born, '%Y/%m/%d')::DATE,
                   street, city, postcode, country,
                   try_strptime(joined, '%Y-%m-%d %H:%M:%S'),
                   CASE lower(trim(status)) WHEN 'active' THEN true WHEN 'inactive' THEN false END,
                   TRY_CAST(trim(value) AS DECIMAL(12, 2))
            FROM raw_legacy
        """)

    @staticmethod
    def clean_and_validate(workspace):
        """Trims and normalises values, then gives every row a reject_reason (NULL = row is valid)."""
        workspace.execute("""
            CREATE OR REPLACE MACRO blank_to_null(raw_text) AS nullif(trim(raw_text), '');
            CREATE OR REPLACE MACRO proper_case(raw_text) AS
                upper(left(blank_to_null(raw_text), 1)) || lower(substr(blank_to_null(raw_text), 2));

            CREATE OR REPLACE TABLE customer_checked AS
            WITH cleaned AS (
                SELECT parsed.source, parsed.source_priority, parsed.source_row, parsed.customer_id_text,
                       TRY_CAST(trim(parsed.customer_id_text) AS BIGINT) AS customer_id,
                       proper_case(parsed.first_name) AS first_name,
                       proper_case(parsed.last_name) AS last_name,
                       lower(blank_to_null(parsed.email)) AS email,
                       blank_to_null(parsed.phone) AS phone,
                       parsed.birth_date,
                       blank_to_null(parsed.street) AS street,
                       blank_to_null(parsed.city) AS city,
                       blank_to_null(parsed.postal_code) AS postal_code,
                       spelling.code AS country_code,
                       parsed.registered_at, parsed.is_active, parsed.lifetime_value
                FROM customer_parsed parsed
                LEFT JOIN country_spelling spelling ON spelling.spelling = lower(trim(parsed.country_text))
            )
            SELECT *,
                   CASE
                       WHEN customer_id IS NULL THEN 'missing or invalid customer_id'
                       WHEN first_name IS NULL OR last_name IS NULL THEN 'missing name'
                       WHEN email IS NULL THEN 'missing email'
                       WHEN NOT regexp_full_match(email, '[^@ ]+@[^@ ]+[.][a-z]{2,}') THEN 'invalid email'
                       WHEN birth_date IS NULL THEN 'invalid birth_date'
                       WHEN country_code IS NULL THEN 'unknown country'
                       WHEN registered_at IS NULL THEN 'invalid registered_at'
                       WHEN is_active IS NULL THEN 'invalid active flag'
                       WHEN lifetime_value IS NULL THEN 'invalid lifetime_value'
                       WHEN lifetime_value < 0 THEN 'negative lifetime_value'
                   END AS reject_reason
            FROM cleaned
        """)
        workspace.execute("""
            CREATE OR REPLACE TABLE rejected_row AS
            SELECT source, source_row, customer_id_text AS customer_id, reject_reason AS reason
            FROM customer_checked
            WHERE reject_reason IS NOT NULL
        """)

    @staticmethod
    def deduplicate(workspace):
        """One row per customer_id: exact duplicates and customers found in several sources are merged.
        The copy from the most trusted source wins (crm > webshop > legacy); `sources` lists all of them."""
        workspace.execute("""
            CREATE OR REPLACE TABLE customer AS
            WITH valid AS (
                SELECT * FROM customer_checked WHERE reject_reason IS NULL
            ),
            ranked AS (
                SELECT *, row_number() OVER (PARTITION BY customer_id
                                             ORDER BY source_priority, source_row) AS rank_in_customer
                FROM valid
            ),
            customer_sources AS (
                SELECT customer_id, string_agg(DISTINCT source, ',' ORDER BY source) AS sources
                FROM valid
                GROUP BY customer_id
            )
            SELECT ranked.customer_id, first_name, last_name, email, phone, birth_date, street, city,
                   postal_code, country_code, registered_at, is_active, lifetime_value, customer_sources.sources
            FROM ranked
            JOIN customer_sources ON customer_sources.customer_id = ranked.customer_id
            WHERE rank_in_customer = 1
            ORDER BY ranked.customer_id
        """)

    # ---------- load ----------

    def load(self, connection, workspace):
        connection.execute("""
            CREATE SCHEMA IF NOT EXISTS multi_format;
            DROP TABLE IF EXISTS multi_format.customer, multi_format.rejected_row, multi_format.country;

            CREATE TABLE multi_format.country (
                code char(2) PRIMARY KEY,
                name text NOT NULL
            );
            CREATE TABLE multi_format.customer (
                customer_id bigint PRIMARY KEY,
                first_name text NOT NULL,
                last_name text NOT NULL,
                email text NOT NULL,
                phone text,
                birth_date date NOT NULL,
                street text,
                city text,
                postal_code text,
                country_code char(2) NOT NULL REFERENCES multi_format.country (code),
                registered_at timestamp NOT NULL,
                is_active boolean NOT NULL,
                lifetime_value numeric(12, 2) NOT NULL,
                sources text NOT NULL
            );
            CREATE TABLE multi_format.rejected_row (
                source text NOT NULL,
                source_row bigint NOT NULL,
                customer_id text,
                reason text NOT NULL
            );
        """)
        self.copy_table(connection, workspace, "country", COUNTRY_COLUMNS)
        self.copy_table(connection, workspace, "customer", CUSTOMER_COLUMNS)
        self.copy_table(connection, workspace, "rejected_row", REJECTED_COLUMNS)

    def copy_table(self, connection, workspace, table: str, columns: tuple[str, ...]):
        """DuckDB writes the table to CSV, PostgreSQL reads it with COPY - much faster than INSERTs."""
        export_path = self.work_dir / f"{table}.csv"
        column_list = ", ".join(columns)
        workspace.execute(f"COPY (SELECT {column_list} FROM {table}) TO {sql_path(export_path)} (FORMAT CSV, HEADER false)")

        with connection.cursor() as cursor, export_path.open("rb") as export_file:
            with cursor.copy(f"COPY multi_format.{table} ({column_list}) FROM STDIN (FORMAT csv)") as copy:
                while chunk := export_file.read(self.copy_chunk_bytes):
                    copy.write(chunk)

    # ---------- run ----------

    def summary(self, workspace) -> dict:
        rows_read = dict(workspace.execute(
            "SELECT source, count(*) FROM customer_checked GROUP BY source ORDER BY min(source_priority)").fetchall())
        rejected = dict(workspace.execute(
            "SELECT reason, count(*) FROM rejected_row GROUP BY reason ORDER BY count(*) DESC").fetchall())
        valid_rows = sum(rows_read.values()) - sum(rejected.values())
        loaded = workspace.execute("SELECT count(*) FROM customer").fetchone()[0]
        return {"rows_read": rows_read, "rejected": rejected, "valid_rows": valid_rows,
                "merged_duplicates": valid_rows - loaded, "customers_loaded": loaded}

    def print_summary(self, summary: dict):
        print("\nRows read:")
        for source, count in summary["rows_read"].items():
            print(f"  {source:<10}{count:>12,}")
        print("Rows rejected:")
        for reason, count in summary["rejected"].items():
            print(f"  {reason:<32}{count:>10,}")
        print(f"Valid rows:          {summary['valid_rows']:>12,}")
        print(f"Merged duplicates:   {summary['merged_duplicates']:>12,}")
        print(f"Customers loaded:    {summary['customers_loaded']:>12,}")

        manifest_path = self.data_dir / MANIFEST_FILE
        if manifest_path.exists():
            unique_customers = json.loads(manifest_path.read_text(encoding="utf-8"))["unique_customers"]
            print(f"Generated customers: {unique_customers:>12,}  (difference = customers with no valid copy)")

    def run(self):
        self.work_dir.mkdir(parents=True, exist_ok=True)
        workspace_path = self.work_dir / "staging.duckdb"
        for stale_file in (workspace_path, workspace_path.with_name(workspace_path.name + ".wal")):
            stale_file.unlink(missing_ok=True)

        workspace = duckdb.connect(str(workspace_path))
        try:
            self.timed("Extract", self.extract, workspace)
            self.timed("Transform", self.transform, workspace)
            with psycopg.connect(self.postgres_dsn) as connection:
                self.timed("Load", self.load, connection, workspace)
            self.print_summary(self.summary(workspace))
        finally:
            workspace.close()

    @staticmethod
    def timed(step_name: str, step, *arguments):
        print(f"{step_name}...", end=" ", flush=True)
        started = time.perf_counter()
        step(*arguments)
        print(f"{time.perf_counter() - started:.1f} s")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Load CSV, JSON Lines and YAML exports into PostgreSQL via DuckDB.")
    parser.add_argument("--dsn", default="host=localhost port=5432 dbname=postgres user=postgres",
                        help="PostgreSQL connection string (password comes from pgpass.conf)")
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).with_name("data"))
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()
    MultiFormatEtl(arguments.dsn, arguments.data_dir).run()
