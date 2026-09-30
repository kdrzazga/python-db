"""Generator + DuckDB extract/transform on a small data set (no PostgreSQL needed)."""
import json

import duckdb
import pytest

from generate_data import DataGenerator
from multi_format_etl import MultiFormatEtl
from reference_data import COUNTRIES, CRM_FILE, LEGACY_FILE, MANIFEST_FILE, WEBSHOP_FILE

KNOWN_REASONS = {
    "missing or invalid customer_id", "missing name", "missing email", "invalid email", "invalid birth_date",
    "unknown country", "invalid registered_at", "invalid active flag", "invalid lifetime_value",
    "negative lifetime_value",
}


def generate(output_dir, seed=7):
    DataGenerator(output_dir, target_megabytes=0.1, seed=seed, dirty_ratio=0.2, overlap_ratio=0.2).run()


@pytest.fixture(scope="module")
def transformed(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("data")
    generate(data_dir)
    etl = MultiFormatEtl("unused", data_dir)
    workspace = duckdb.connect(":memory:")
    etl.extract(workspace)
    etl.transform(workspace)
    yield data_dir, etl, workspace
    workspace.close()


def scalar(workspace, query):
    return workspace.execute(query).fetchone()[0]


class TestGenerator:
    def test_same_seed_gives_identical_files(self, tmp_path):
        generate(tmp_path / "first")
        generate(tmp_path / "second")
        for file_name in (CRM_FILE, WEBSHOP_FILE, LEGACY_FILE, MANIFEST_FILE):
            assert (tmp_path / "first" / file_name).read_bytes() == (tmp_path / "second" / file_name).read_bytes()

    def test_manifest_counts_records_of_every_source(self, transformed):
        data_dir, _, _ = transformed
        manifest = json.loads((data_dir / MANIFEST_FILE).read_text(encoding="utf-8"))
        assert set(manifest["sources"]) == {"crm", "webshop", "legacy"}
        assert all(report["records"] > 0 for report in manifest["sources"].values())


class TestTransform:
    def test_every_source_is_read_completely(self, transformed):
        data_dir, etl, workspace = transformed
        manifest = json.loads((data_dir / MANIFEST_FILE).read_text(encoding="utf-8"))
        rows_read = etl.summary(workspace)["rows_read"]
        assert rows_read == {source: report["records"] for source, report in manifest["sources"].items()}

    def test_customer_ids_are_unique(self, transformed):
        _, _, workspace = transformed
        assert scalar(workspace, "SELECT count(*) - count(DISTINCT customer_id) FROM customer") == 0

    def test_loaded_customers_do_not_exceed_generated_ones(self, transformed):
        data_dir, _, workspace = transformed
        manifest = json.loads((data_dir / MANIFEST_FILE).read_text(encoding="utf-8"))
        assert 0 < scalar(workspace, "SELECT count(*) FROM customer") <= manifest["unique_customers"]

    def test_country_spellings_are_normalised_to_codes(self, transformed):
        _, _, workspace = transformed
        codes = {row[0] for row in workspace.execute("SELECT DISTINCT country_code FROM customer").fetchall()}
        assert codes <= {country.code for country in COUNTRIES}
        assert scalar(workspace, "SELECT count(*) FROM rejected_row WHERE reason = 'unknown country'") == 0

    def test_padded_names_are_trimmed_and_capitalised(self, transformed):
        _, _, workspace = transformed
        assert scalar(workspace, """
            SELECT count(*) FROM customer
            WHERE first_name <> trim(first_name) OR left(first_name, 1) <> upper(left(first_name, 1))
        """) == 0

    def test_dirty_rows_are_rejected_with_known_reasons(self, transformed):
        _, _, workspace = transformed
        reasons = {row[0] for row in workspace.execute("SELECT DISTINCT reason FROM rejected_row").fetchall()}
        assert reasons and reasons <= KNOWN_REASONS

    def test_customers_found_in_several_sources_are_merged(self, transformed):
        _, _, workspace = transformed
        assert scalar(workspace, "SELECT count(*) FROM customer WHERE sources LIKE '%,%'") > 0
