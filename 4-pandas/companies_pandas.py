import pandas as pd


def create_companies_df():
    rows = [
        (1, "Apple", "United States", "Technology"),
        (2, "Microsoft", "United States", "Technology"),
        (3, "Saudi Aramco", "Saudi Arabia", "Energy"),
        (4, "Shell", "United Kingdom", "Energy"),
        (5, "Toyota", "Japan", "Automotive"),
        (6, "Volkswagen", "Germany", "Automotive"),
        (7, "Samsung Electronics", "South Korea", "Technology"),
        (8, "TSMC", "Taiwan", "Semiconductors"),
        (9, "Nestle", "Switzerland", "Food"),
        (10, "LVMH", "France", "Luxury Goods"),
    ]
    return pd.DataFrame(rows, columns=["company_id", "name", "country", "industry"])


def process_industries_outside_us(companies):
    #  sql/companies.sql:
    #   SELECT industry, COUNT(*) AS companies, STRING_AGG(name, ', ') AS names
    #   FROM companies
    #   WHERE country <> 'United States'
    #   GROUP BY industry
    #   HAVING COUNT(*) >= 2;
    print("Industries with at least two companies based outside the US:")
    result = (
        companies[companies["country"] != "United States"]          # WHERE
        .groupby("industry", as_index=False)                         # GROUP BY
        .agg(
            companies=("name", "count"),
            names=("name", ", ".join),                               # STRING_AGG
        )
        .query("companies >= 2")                                     # HAVING
    )
    print(result.to_string(index=False))


info = '''pandas vs Spark:
1. No session, no JVM: pandas is a plain Python library, the DataFrame lives in this process's memory.
2. Eager: every line runs immediately - there is no plan and no action like show() or collect().
3. Any Python function can be an aggregate: ", ".join replaces Spark's concat_ws(collect_list(...)).
4. Limit: all data must fit in RAM on one machine.'''

print(info)

process_industries_outside_us(create_companies_df())
