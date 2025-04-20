import pandas as pd
from datetime import datetime, timedelta
from framework_tester import FrameworkTester
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

class PandaTester(FrameworkTester):

    # def __init__(self):
    #     #self.duckdb_con = con
    #     """
    #     Maybe each table could be an attribute so tables aren't loaded during a query.
    #     But having all the tables in memory could be an issue as the dataset scales.
    #     """

    def tpc_q1(self):
        lineitem = self.lineitem

        var1 = pd.Timestamp(1998, 9, 2)

        filt = line_item_ds[line_item_ds["l_shipdate"] <= var1]

        # This is lenient towards pandas as normally an optimizer should decide
        # that this could be computed before the groupby aggregation.
        # Other implementations don't enjoy this benefit.
        filt["disc_price"] = filt.l_extendedprice * (1.0 - filt.l_discount)
        filt["charge"] = (
            filt.l_extendedprice * (1.0 - filt.l_discount) * (1.0 + filt.l_tax)
        )

        gb = filt.groupby(["l_returnflag", "l_linestatus"], as_index=False)
        agg = gb.agg(
            sum_qty=pd.NamedAgg(column="l_quantity", aggfunc="sum"),
            sum_base_price=pd.NamedAgg(column="l_extendedprice", aggfunc="sum"),
            sum_disc_price=pd.NamedAgg(column="disc_price", aggfunc="sum"),
            sum_charge=pd.NamedAgg(column="charge", aggfunc="sum"),
            avg_qty=pd.NamedAgg(column="l_quantity", aggfunc="mean"),
            avg_price=pd.NamedAgg(column="l_extendedprice", aggfunc="mean"),
            avg_disc=pd.NamedAgg(column="l_discount", aggfunc="mean"),
            count_order=pd.NamedAgg(column="l_orderkey", aggfunc="size"),
        )

        result_df = agg.sort_values(["l_returnflag", "l_linestatus"])

        print(result_df)
        return result_df  # type: ignore[no-any-return]
    
    def tpc_q3(self):
        customer_ds = self.duckdb_con.execute("SELECT * FROM customer").fetchdf()
        line_item_ds = self.duckdb_con.execute("SELECT * FROM lineitem").fetchdf()
        orders_ds = self.duckdb_con.execute("SELECT * FROM orders").fetchdf()

        var1 = "BUILDING"
        var2 = pd.Timestamp("1995-03-15")

        fcustomer = customer_ds[customer_ds["c_mktsegment"] == var1]

        jn1 = fcustomer.merge(orders_ds, left_on="c_custkey", right_on="o_custkey")
        jn2 = jn1.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")

        jn2 = jn2[jn2["o_orderdate"] < var2]
        jn2 = jn2[jn2["l_shipdate"] > var2]
        jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)

        gb = jn2.groupby(
            ["o_orderkey", "o_orderdate", "o_shippriority"], as_index=False
        )
        agg = gb["revenue"].sum()

        sel = agg.loc[:, ["o_orderkey", "revenue", "o_orderdate", "o_shippriority"]]
        sel = sel.rename(columns={"o_orderkey": "l_orderkey"})

        sorted = sel.sort_values(by=["revenue", "o_orderdate"], ascending=[False, True])
        result_df = sorted.head(10)

        print(result_df)
        return


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
        part = self.duckdb_con.execute("SELECT * FROM part").fetchdf()
        # print(part.head())
        reduced = part.drop(columns=["COMMENT", "RETAILPRICE"])
        print("Part table without COMMENT and RETAILPRICE:\n", reduced.head())

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