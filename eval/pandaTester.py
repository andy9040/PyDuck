import pandas as pd
from datetime import datetime, timedelta
import duckdb
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
        line_item_ds = self.duckdb_con.execute("SELECT * FROM lineitem").fetchdf()

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
        customer = self.duckdb_con.execute("SELECT * FROM customer").fetchdf()
        sample = customer.sample(5)
        print("Sample 5 customers:\n", sample)

    def test_drop_duplicates(self):
        supplier = self.duckdb_con.execute("SELECT * FROM supplier").fetchdf()
        deduped = supplier.drop_duplicates(subset=["nationkey"])
        print("Suppliers with unique nationkeys:\n", deduped)

    def test_drop_columns(self):
        part = self.duckdb_con.execute("SELECT * FROM part").fetchdf()
        # print(part.head())
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
p.tpc_q1()
p.tpc_q3()
p.test_drop_columns()
p.test_drop_duplicates()
p.test_dropna()
p.test_fillna()
p.test_get_dummies()
p.test_groupby_agg()
p.test_isna_sum()
p.test_sample()
