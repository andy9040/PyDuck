from pyduck.quack import Quack
from datetime import datetime, timedelta
import duckdb
from framework_tester import FrameworkTester

class PyDuckTester(FrameworkTester):
    def tpc_q1(self):

        # Step 1: Set up the Quack object
        lineitem = Quack("lineitem", self.duckdb_con)

        # Step 2: Build and execute query
        cutoff_date = (datetime.strptime("1998-12-01", "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")

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
            .to_df()
        )
    
    def test_sample(self):
        customer = Quack("customer", self.duckdb_con)
        result = customer.sample(n=5).to_df()

    def test_drop_duplicates(self):
        supplier = Quack("supplier", self.duckdb_con)
        result = supplier.drop_duplicates(subset="s_nationkey").to_df()

    def test_drop_columns(self):
        part = Quack("part", self.duckdb_con)
        result = part.drop(columns=["p_comment", "p_retailprice"]).to_df()

    def test_fillna(self):
        nation = Quack("nation", self.duckdb_con)
        result = nation.fillna({"n_comment": "No comment"}).to_df()

    def test_dropna(self):
        orders = Quack("orders", self.duckdb_con)
        result = orders.dropna().to_df()

    def test_isna_sum(self):
        orders = Quack("orders", self.duckdb_con)
        result = orders.isna().sum().to_df()

    def test_get_dummies(self):
        customer = Quack("customer", self.duckdb_con)
        customer.get_dummies("c_mktsegment", values=["BUILDING", "AUTOMOBILE", "MACHINERY"])
        result = customer.to_df()

    def test_groupby_agg(self):
        lineitem = Quack("lineitem", self.duckdb_con)
        result = (
            lineitem
            .groupby("l_returnflag")
            .agg({
                "l_quantity": "sum",
                "l_extendedprice": "avg",
                "l_orderkey": "count"
            })
            .to_df()
        )

