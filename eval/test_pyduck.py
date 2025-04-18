from pyduck.quack import Quack
from datetime import datetime, timedelta
import duckdb

# Step 1: Set up the Quack object
con = duckdb.connect('tpch.duckdb')
lineitem = Quack("lineitem", con)

# Step 2: Build and execute query
cutoff_date = (datetime.strptime("1998-12-01", "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

result = (
    lineitem
    .filter(f"l_shipdate <= DATE '{cutoff_date}'")
    .assign(
        disc_price="l_extendedprice * (1 - l_discount)",
        charge="l_extendedprice * (1 - l_discount) * (1 + l_tax)"
    )
    .groupby(["l_returnflag", "l_linestatus"])
    .agg({
        "l_quantity": "sum",
        "l_extendedprice": "sum",
        "disc_price": "sum",
        "charge": "sum",
        "l_quantity": "avg",
        "l_extendedprice": "avg",
        "l_discount": "avg",
        "l_quantity": "count"
    })
    .sort_values(by=["l_returnflag", "l_linestatus"])
    .head(1)
    .to_df()
)

print(result)
