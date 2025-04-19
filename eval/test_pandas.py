import pandas as pd
from datetime import datetime, timedelta
import duckdb


# Example: Load DuckDB result into a pandas DataFrame
con = duckdb.connect('tpch.duckdb')
lineitem = con.execute("SELECT * FROM lineitem").fetchdf()

# Ensure l_shipdate is in datetime format
lineitem["l_shipdate"] = pd.to_datetime(lineitem["l_shipdate"])

# Apply the date filter: l_shipdate <= '1998-12-01' - interval 1 day
cutoff_date = datetime.strptime("1998-12-01", "%Y-%m-%d") - timedelta(days=1)
filtered = lineitem[lineitem["l_shipdate"] <= cutoff_date].copy()

# Precompute derived columns
filtered["disc_price"] = filtered["l_extendedprice"] * (1 - filtered["l_discount"])
filtered["charge"] = filtered["disc_price"] * (1 + filtered["l_tax"])

# Perform groupby and aggregation
agg = (
    filtered.groupby(["l_returnflag", "l_linestatus"])
    .agg(
        sum_qty=("l_quantity", "sum"),
        sum_base_price=("l_extendedprice", "sum"),
        sum_disc_price=("disc_price", "sum"),
        sum_charge=("charge", "sum"),
        avg_qty=("l_quantity", "mean"),
        avg_price=("l_extendedprice", "mean"),
        avg_disc=("l_discount", "mean"),
        count_order=("l_quantity", "count")  # or use any column, as long as it's not null
    )
    .reset_index()
    .sort_values(by=["l_returnflag", "l_linestatus"])
    .head(1)  # LIMIT 1
)

print(agg)
