class DataFrameFactory:

	@staticmethod
	def create_hello_df() -> tuple:

		result = ([("Hello, Spark!", 12), ("Ala ma kota", len("Ala ma kota")), ("Seventeen", 17)
		          , ("One hundred forty five", 145), ("Five millions", 5_000_000)]
		          , ["message", "number"])
		return result
