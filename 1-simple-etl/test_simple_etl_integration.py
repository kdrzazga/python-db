"""Integration tests: run PersonTaxEtl against a real PostgreSQL server.

Each test session creates a throwaway database, seeds it from sql/person.sql and drops it at the end,
so your real tables are never touched. Tests are skipped when the server is not reachable.
Connection settings come from PG_TEST_DSN (default below); the password comes from pgpass.conf.
"""
import os
import uuid
from decimal import Decimal
from pathlib import Path

import duckdb
import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from simple_etl import PersonTaxEtl

pytestmark = pytest.mark.integration

DEFAULT_ADMIN_DSN = "host=localhost port=5432 dbname=postgres user=postgres"
SEED_SCRIPT = Path(__file__).resolve().parent.parent / "sql" / "person.sql"


@pytest.fixture(scope="session")
def test_database_dsn():
    admin_dsn = os.environ.get("PG_TEST_DSN", DEFAULT_ADMIN_DSN)
    try:
        admin_connection = psycopg.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError as error:
        pytest.skip(f"PostgreSQL not reachable ({error})")

    database_name = f"etl_test_{uuid.uuid4().hex[:8]}"
    admin_connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
    try:
        yield make_conninfo(admin_dsn, dbname=database_name)
    finally:
        admin_connection.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(database_name))
        )
        admin_connection.close()


@pytest.fixture
def seeded_dsn(test_database_dsn):
    """Recreates person/tax from the seed script before every test, so tests don't affect each other."""
    with psycopg.connect(test_database_dsn, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS report CASCADE")
        connection.execute(SEED_SCRIPT.read_text(encoding="utf-8"))
    return test_database_dsn


def fetch_all(dsn, query):
    with psycopg.connect(dsn) as connection:
        return connection.execute(query).fetchall()


class TestPersonTaxEtlOnPostgres:
    def test_loads_one_report_row_per_person(self, seeded_dsn):
        PersonTaxEtl(seeded_dsn).run()

        person_count = fetch_all(seeded_dsn, "SELECT count(*) FROM person")[0][0]
        report_count = fetch_all(seeded_dsn, "SELECT count(*) FROM report.person_tax")[0][0]
        assert report_count == person_count == 100

    def test_report_matches_join_computed_in_postgres(self, seeded_dsn):
        PersonTaxEtl(seeded_dsn).run()

        expected = fetch_all(seeded_dsn, """
            SELECT p.id,
                   p.name || ' ' || p.last_name,
                   t.income,
                   CASE WHEN t.income < 20000  THEN 'low'
                        WHEN t.income < 100000 THEN 'mid'
                        ELSE 'high' END
            FROM person p JOIN tax t ON t.id = p.tax_id
            ORDER BY p.id
        """)
        actual = fetch_all(seeded_dsn, "SELECT * FROM report.person_tax ORDER BY person_id")
        assert actual == expected

    def test_first_seeded_person(self, seeded_dsn):
        PersonTaxEtl(seeded_dsn).run()

        first_row = fetch_all(seeded_dsn, "SELECT * FROM report.person_tax WHERE person_id = 1")
        assert first_row == [(1, "James Smith", Decimal("123000.33"), "high")]

    def test_rerun_replaces_report_instead_of_duplicating(self, seeded_dsn):
        etl = PersonTaxEtl(seeded_dsn)

        etl.run()
        etl.run()

        assert fetch_all(seeded_dsn, "SELECT count(*) FROM report.person_tax")[0][0] == 100

    def test_person_without_tax_row_is_left_out(self, seeded_dsn):
        with psycopg.connect(seeded_dsn) as connection:
            connection.execute("INSERT INTO person (name, last_name, tax_id) VALUES ('No', 'Tax', NULL)")

        PersonTaxEtl(seeded_dsn).run()

        assert fetch_all(seeded_dsn, "SELECT * FROM report.person_tax WHERE full_name = 'No Tax'") == []

    def test_exports_parquet_with_same_rows(self, seeded_dsn, tmp_path):
        parquet_path = tmp_path / "person_tax.parquet"

        PersonTaxEtl(seeded_dsn, parquet_path=str(parquet_path)).run()

        exported_count = duckdb.execute(
            f"SELECT count(*) FROM read_parquet('{parquet_path.as_posix()}')"
        ).fetchone()[0]
        assert exported_count == 100
