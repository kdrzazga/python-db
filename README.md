# python-db

SETUP:
1. Install POSTGRES-SQL and configure it
2. Run setup-pgpass.bat and give parameters defined during PostgreSQL install (host, port, user and password)
3. in PostgreSQL run seed script: sql/person.sql
4. Start PostgrSQL / make sure it's running
4. Run:

`pytest simple_etl.py ` to test code on mocked DB

`pytest test_simple_etl_integration.py` to test on real DB
