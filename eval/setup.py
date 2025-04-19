import duckdb

# Connect to DuckDB (in-memory or on-disk)
con = duckdb.connect('tpch.duckdb')  # use ':memory:' for in-memory DB

# Load the TPC-H extension
con.execute("INSTALL tpch;")
con.execute("LOAD tpch;")

# Create TPC-H tables at a chosen scale factor (e.g., 0.1 = ~100MB)
con.execute("CALL dbgen(sf=0.1);")  # you can increase sf to 1, 10, etc.

result = con.execute("SELECT * FROM lineitem LIMIT 5").fetchdf()
print(result)
