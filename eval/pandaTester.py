import pandas as pd
from datetime import datetime, timedelta
import duckdb
from framework_tester import FrameworkTester

class PandaTester(FrameworkTester):

    def tpc_q1(self):
        # Example: Load DuckDB result into a pandas DataFrame
        lineitem = self.duckdb_con.execute("SELECT * FROM lineitem").fetchdf()

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

    def test_sample(self):
        customer = self.duckdb_con.execute("SELECT * FROM customer").fetchdf()
        sample = customer.sample(5)
        print("Sample 5 customers:\n", sample)

    def test_drop_duplicates(self):
        supplier = self.duckdb_con.execute("SELECT * FROM supplier").fetchdf()
        deduped = supplier.drop_duplicates(subset=["nationkey"])
        print("Suppliers with unique nationkeys:\n", deduped)

    def test_drop_columns(self):
        part = self.duckdb_con.execute("SELECT * FROM part").fetchdf()
        reduced = part.drop(columns=["COMMENT", "RETAILPRICE"])
        print("Part table without COMMENT and RETAILPRICE:\n", reduced.head())

    def test_fillna(self):
        nation = self.duckdb_con.execute("SELECT * FROM nation").fetchdf()
        nation["COMMENT"] = nation["COMMENT"].fillna("No comment")
        print("Nation table with COMMENT nulls filled:\n", nation.head())

    def test_dropna(self):
        orders = self.duckdb_con.execute("SELECT * FROM orders").fetchdf()
        clean_orders = orders.dropna(subset=["CLERK"])
        print("Orders with non-null CLERK values:\n", clean_orders.head())

    def test_isna_sum(self):
        orders = self.duckdb_con.execute("SELECT * FROM orders").fetchdf()
        missing_counts = orders.isna().sum()
        print("Missing values per column in ORDERS:\n", missing_counts)

    def test_get_dummies(self):
        customer = self.duckdb_con.execute("SELECT * FROM customer").fetchdf()
        dummies = pd.get_dummies(customer[["MKTSEGMENT"]])
        print("Market Segment One-Hot Encoding:\n", dummies.head())

    def test_groupby_agg(self):
        lineitem = self.duckdb_con.execute("SELECT * FROM lineitem").fetchdf()
        # Convert date columns if needed
        lineitem["SHIPDATE"] = pd.to_datetime(lineitem["SHIPDATE"])
        grouped = (
            lineitem.groupby(["RETURNFLAG"])
            .agg(
                total_quantity=("QUANTITY", "sum"),
                avg_price=("EXTENDEDPRICE", "mean"),
                order_count=("ORDERKEY", "nunique")
            )
            .reset_index()
        )
        print("Lineitem grouped by RETURNFLAG:\n", grouped)



con = duckdb.connect('tpch.duckdb')
p = PandaTester(con)
p.test_drop_columns()
p.test_drop_duplicates()
p.test_dropna()
p.test_fillna()
p.test_get_dummies()
p.test_groupby_agg()
p.test_isna_sum()
p.test_sample()
