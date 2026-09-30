DROP TABLE IF EXISTS countries;

CREATE TABLE countries (
    id          SERIAL PRIMARY KEY,
    country     TEXT    NOT NULL,
    continent   TEXT    NOT NULL,
    population  INTEGER NOT NULL,  -- millions
    gdp         INTEGER NOT NULL   -- billions of USD
);

INSERT INTO countries (country, continent, population, gdp) VALUES
    ('India',                            'Asia',          1464,  4190),
    ('China',                            'Asia',          1416, 19400),
    ('United States',                    'North America',  347, 30510),
    ('Indonesia',                        'Asia',           286,  1430),
    ('Pakistan',                         'Asia',           255,   410),
    ('Nigeria',                          'Africa',         238,   188),
    ('Brazil',                           'South America',  213,  2260),
    ('Bangladesh',                       'Asia',           176,   467),
    ('Russia',                           'Europe',         100,  2000),
    ('Russia',                           'Asia',           44,  540),
    ('Ethiopia',                         'Africa',         135,   109),
    ('Mexico',                           'North America',  132,  1860),
    ('Japan',                            'Asia',           123,  4280),
    ('Egypt',                            'Africa',         118,   347),
    ('Philippines',                      'Asia',           117,   497),
    ('Democratic Republic of the Congo', 'Africa',         113,    79),
    ('Vietnam',                          'Asia',           102,   506),
    ('Iran',                             'Asia',            92,   357),
    ('Turkey',                           'Europe',          1,  40),
    ('Turkey',                           'Asia',            87,  1400),
    ('Germany',                          'Europe',          84,  4740),
    ('Thailand',                         'Asia',            72,   559),
    ('Poland',                           'Europe',          38,   915);


WITH countries_with_wealth AS (
    SELECT *,
           CASE WHEN gdp::numeric / population >= 20 THEN 'rich'
                WHEN gdp::numeric / population >= 5  THEN 'middle'
                ELSE 'poor' END AS wealth
    FROM countries
)
SELECT continent, wealth, COUNT(*) AS countries
FROM countries_with_wealth
GROUP BY continent, wealth
HAVING COUNT(*) >= 2
ORDER BY continent, wealth;
