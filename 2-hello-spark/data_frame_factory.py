class DataFrameFactory:

	@staticmethod
	def create_hello_df() -> tuple:

		result = ([("Hello, Spark!", 12), ("Ala ma kota", len("Ala ma kota")), ("Seventeen", 17)
		          , ("One hundred forty five", 145), ("Five millions", 5_000_000)]
		          , ["message", "number"])
		return result

	@staticmethod
	def create_countries_df() -> tuple:

		countries = ([
			("India", "Asia", 1464, 4190),
			("China", "Asia", 1416, 19400),
			("United States", "North America", 347, 30510),
			("Indonesia", "Asia", 286, 1430),
			("Pakistan", "Asia", 255, 410),
			("Nigeria", "Africa", 238, 188),
			("Brazil", "South America", 213, 2260),
			("Bangladesh", "Asia", 176, 467),
			("Russia", "Europe", 144, 2540),
			("Russia", "Asia", 144, 2540),
			("Ethiopia", "Africa", 135, 109),
			("Mexico", "North America", 132, 1860),
			("Japan", "Asia", 123, 4280),
			("Egypt", "Africa", 118, 347),
			("Philippines", "Asia", 117, 497),
			("Democratic Republic of the Congo", "Africa", 113, 79),
			("Vietnam", "Asia", 102, 506),
			("Iran", "Asia", 92, 357),
			("Turkey", "Europe", 88, 1440),
			("Turkey", "Asia", 88, 1440),
			("Germany", "Europe", 84, 4740),
			("Thailand", "Asia", 72, 559),
			("Poland", "Europe", 38, 915),
			],
			["country", "continent", "population", "GPD"] # population in millions, GDP in billions of USD
		)

		return countries

	@staticmethod
	def create_companies_df() -> tuple:

		companies = ([
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
			],
			["company_id", "name", "country", "industry"]
		)

		return companies
