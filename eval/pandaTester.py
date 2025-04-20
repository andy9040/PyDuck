import pandas as pd
from datetime import datetime, timedelta
from framework_tester import FrameworkTester

class PandaTester(FrameworkTester):

    def tpc_q1(self):
        lineitem = self.lineitem

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
        customer = self.customer
        sample = customer.sample(5)

    def test_drop_duplicates(self):
        supplier = self.supplier
        # print(supplier.head(5))
        deduped = supplier.drop_duplicates(subset=["s_nationkey"])
        # print("Suppliers with unique nationkeys:\n", deduped)

    def test_drop_columns(self):
        part = self.part
        reduced = part.drop(columns=["p_comment", "p_retailprice"])

    def test_fillna(self):
        nation = self.nation
        nation["n_comment"] = nation["n_comment"].fillna("No comment")

    def test_dropna(self):
        orders = self.orders
        clean_orders = orders.dropna(subset=["o_clerk"])

    def test_isna_sum(self):
        orders = self.orders
        missing_counts = orders.isna().sum()

    def test_get_dummies(self):
        customer = self.customer
        dummies = pd.get_dummies(customer[["c_mktsegment"]])

    def test_groupby_agg(self):
        lineitem = self.lineitem
        # Convert date columns if needed
        lineitem["l_shipdate"] = pd.to_datetime(lineitem["l_shipdate"])
        grouped = (
            lineitem.groupby(["l_returnflag"])
            .agg(
                total_quantity=("l_quantity", "sum"),
                avg_price=("l_extendedprice", "mean"),
                order_count=("l_orderkey", "nunique")
            )
            .reset_index()
        )