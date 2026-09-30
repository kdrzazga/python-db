drop table if exists financials CASCADE;
drop table if exists companies CASCADE;

create table companies(
company_id int PRIMARY KEY,
name varchar(200) NOT NULL,
country varchar(100),  -- headquarters country
industry varchar(100)
);

create table financials(
financial_id int PRIMARY KEY,
company_id int NOT NULL REFERENCES companies(company_id),
year int NOT NULL,
revenue_usd decimal(18,2),     -- annual revenue
net_income_usd decimal(18,2),  -- annual profit
market_cap_usd decimal(18,2),  -- market capitalization
UNIQUE (company_id, year)
);


-- Figures are approximate, fiscal year 2024, converted to USD
INSERT INTO companies(company_id, name, country, industry) VALUES
(1, 'Apple', 'United States', 'Technology'),
(2, 'Microsoft', 'United States', 'Technology'),
(3, 'Saudi Aramco', 'Saudi Arabia', 'Energy'),
(4, 'Shell', 'United Kingdom', 'Energy'),
(5, 'Toyota', 'Japan', 'Automotive'),
(6, 'Volkswagen', 'Germany', 'Automotive'),
(7, 'Samsung Electronics', 'South Korea', 'Technology'),
(8, 'TSMC', 'Taiwan', 'Semiconductors'),
(9, 'Nestle', 'Switzerland', 'Food'),
(10, 'LVMH', 'France', 'Luxury Goods');


INSERT INTO financials(financial_id, company_id, year, revenue_usd, net_income_usd, market_cap_usd) VALUES
(1, 1, 2024, 391035000000.00, 93736000000.00, 3400000000000.00),
(2, 2, 2024, 245122000000.00, 88136000000.00, 3100000000000.00),
(3, 3, 2024, 480400000000.00, 106200000000.00, 1800000000000.00),
(4, 4, 2024, 284300000000.00, 16100000000.00, 200000000000.00),
(5, 5, 2024, 312000000000.00, 34000000000.00, 250000000000.00),
(6, 6, 2024, 351000000000.00, 13400000000.00, 46000000000.00),
(7, 7, 2024, 216000000000.00, 24000000000.00, 240000000000.00),
(8, 8, 2024, 90000000000.00, 36500000000.00, 1000000000000.00),
(9, 9, 2024, 103000000000.00, 12400000000.00, 215000000000.00),
(10, 10, 2024, 91600000000.00, 13600000000.00, 330000000000.00);

-- list industries with at least two companies based outside the US
SELECT industry,
       COUNT(*) AS companies,
       STRING_AGG(name, ', ') AS names
FROM companies
WHERE country <> 'United States'
GROUP BY industry
HAVING COUNT(*) >= 2;