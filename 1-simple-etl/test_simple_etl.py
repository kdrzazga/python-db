from decimal import Decimal

import duckdb
import pytest

import simple_etl
from simple_etl import PersonTaxEtl


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def executemany(self, sql, rows):
        self.connection.inserted_rows.extend(rows)


class FakeConnection:
    """Stands in for a psycopg connection: answers SELECTs by table name and records everything else."""

    def __init__(self, rows_by_table=None):
        self.rows_by_table = rows_by_table or {}
        self.executed_sql = []
        self.inserted_rows = []

    def execute(self, sql):
        self.executed_sql.append(sql)
        for table_name, rows in self.rows_by_table.items():
            if f"FROM {table_name}" in sql:
                return FakeResult(rows)
        return FakeResult([])

    def cursor(self):
        return FakeCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


@pytest.fixture
def etl():
    return PersonTaxEtl("host=localhost dbname=test")


def fetch_person_tax(workspace):
    return workspace.execute(
        "SELECT person_id, full_name, income, income_bracket FROM person_tax ORDER BY person_id"
    ).fetchall()


class TestExtract:
    def test_returns_person_and_tax_rows(self, etl):
        persons = [(1, "James", "Smith", 482)]
        taxes = [(482, Decimal("123000.33"))]
        connection = FakeConnection({"person": persons, "tax": taxes})

        assert etl.extract(connection) == (persons, taxes)


class TestTransform:
    def test_joins_person_with_tax_and_builds_full_name(self, etl):
        workspace = etl.transform(
            [(1, "James", "Smith", 482)],
            [(482, Decimal("123000.33"))],
        )

        assert fetch_person_tax(workspace) == [(1, "James Smith", Decimal("123000.33"), "high")]

    @pytest.mark.parametrize(
        ("income", "expected_bracket"),
        [
            (Decimal("0.00"), "low"),
            (Decimal("19999.99"), "low"),
            (Decimal("20000.00"), "mid"),
            (Decimal("99999.99"), "mid"),
            (Decimal("100000.00"), "high"),
        ],
    )
    def test_income_bracket_boundaries(self, etl, income, expected_bracket):
        workspace = etl.transform([(1, "Mary", "Johnson", 19)], [(19, income)])

        assert fetch_person_tax(workspace)[0][3] == expected_bracket

    def test_skips_person_without_tax_row(self, etl):
        workspace = etl.transform(
            [(1, "James", "Smith", 482), (2, "Mary", "Johnson", 999)],
            [(482, Decimal("1503.00"))],
        )

        assert [row[0] for row in fetch_person_tax(workspace)] == [1]


class TestLoad:
    @pytest.fixture
    def workspace(self, etl):
        return etl.transform(
            [(1, "James", "Smith", 482), (2, "Mary", "Johnson", 19)],
            [(482, Decimal("123000.33")), (19, Decimal("12.33"))],
        )

    def test_recreates_report_table_and_inserts_rows(self, etl, workspace):
        connection = FakeConnection()

        etl.load(connection, workspace)

        assert any("DROP TABLE IF EXISTS report.person_tax" in sql for sql in connection.executed_sql)
        assert any("CREATE TABLE report.person_tax" in sql for sql in connection.executed_sql)
        assert sorted(connection.inserted_rows) == [
            (1, "James Smith", Decimal("123000.33"), "high"),
            (2, "Mary Johnson", Decimal("12.33"), "low"),
        ]

    def test_writes_parquet_when_path_given(self, workspace, tmp_path):
        parquet_path = tmp_path / "person_tax.parquet"
        etl = PersonTaxEtl("host=localhost dbname=test", parquet_path=str(parquet_path))

        etl.load(FakeConnection(), workspace)

        exported = duckdb.execute(
            f"SELECT person_id FROM read_parquet('{parquet_path.as_posix()}') ORDER BY person_id"
        ).fetchall()
        assert exported == [(1,), (2,)]

    def test_skips_parquet_when_no_path(self, etl, workspace, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        etl.load(FakeConnection(), workspace)

        assert list(tmp_path.iterdir()) == []


class TestRun:
    def test_runs_extract_transform_load_on_one_connection(self, etl, monkeypatch):
        connection = FakeConnection({
            "person": [(1, "James", "Smith", 482)],
            "tax": [(482, Decimal("50000.00"))],
        })
        used_dsns = []

        def fake_connect(dsn):
            used_dsns.append(dsn)
            return connection

        monkeypatch.setattr(simple_etl.psycopg, "connect", fake_connect)

        etl.run()

        assert used_dsns == ["host=localhost dbname=test"]
        assert connection.inserted_rows == [(1, "James Smith", Decimal("50000.00"), "mid")]
