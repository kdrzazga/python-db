# python-db

SETUP:
1. Install POSTGRES-SQL and configure it
2. Run setup-pgpass.bat and give parameters defined during PostgreSQL install (host, port, user and password)
3. in PostgreSQL run seed script: sql/person.sql
4. Start PostgrSQL / make sure it's running
5. Run:

`pytest simple_etl.py ` to test code on mocked DB

`pytest test_simple_etl_integration.py` to test on real DB

## 3-mul-formats

`3-mul-formats` is an ETL exercise that loads the same kind of data - customers with names, addresses and countries - from three differently shaped sources: a `;`-separated CSV, a JSON Lines file and a multi-document YAML stream.

`generate_data.py` creates about 200 MB per file from a fixed seed (identical output on every run), with deliberately dirty rows and customers repeated across sources, so only the generator is committed and `data/` stays out of git.

`multi_format_etl.py` reads all three files into a DuckDB workspace, maps each source's conventions (date formats, decimal commas, Y/N flags, country spellings) onto one schema, rejects invalid rows with a reason and merges duplicates by `customer_id`.

The result is bulk-loaded with `COPY` into the PostgreSQL schema `multi_format` (tables `customer`, `country` and `rejected_row`).

`python 3-mul-formats/generate_data.py` (options: `--size-mb`, `--seed`, `--dirty-ratio`, `--zip`), then `python 3-mul-formats/multi_format_etl.py`; `pytest 3-mul-formats` tests the generator and transform on a small data set without PostgreSQL.
