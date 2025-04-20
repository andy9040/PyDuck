import pandas as pd
from datetime import datetime, timedelta
from eval.frameworkTester import FrameworkTester
from datetime import date
from typing import TYPE_CHECKING
import duckdb

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

        filt = lineitem[lineitem["l_shipdate"] <= var1]

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

    
    # IGNORE FOR NOW
    # def tpc_q3(self):
    #     customer_ds = self.duckdb_con.execute("SELECT * FROM customer").fetchdf()
    #     line_item_ds = self.duckdb_con.execute("SELECT * FROM lineitem").fetchdf()
    #     orders_ds = self.duckdb_con.execute("SELECT * FROM orders").fetchdf()

    #     var1 = "BUILDING"
    #     var2 = pd.Timestamp("1995-03-15")

    #     fcustomer = customer_ds[customer_ds["c_mktsegment"] == var1]

    #     jn1 = fcustomer.merge(orders_ds, left_on="c_custkey", right_on="o_custkey")
    #     jn2 = jn1.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")

    #     jn2 = jn2[jn2["o_orderdate"] < var2]
    #     jn2 = jn2[jn2["l_shipdate"] > var2]
    #     jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)

    #     gb = jn2.groupby(
    #         ["o_orderkey", "o_orderdate", "o_shippriority"], as_index=False
    #     )
    #     agg = gb["revenue"].sum()

    #     sel = agg.loc[:, ["o_orderkey", "revenue", "o_orderdate", "o_shippriority"]]
    #     sel = sel.rename(columns={"o_orderkey": "l_orderkey"})

    #     sorted = sel.sort_values(by=["revenue", "o_orderdate"], ascending=[False, True])
    #     result_df = sorted.head(10)

    #     print(result_df)
    #     return
    
    def tpc_q4(self):
        line_item_ds = self.lineitem
        orders_ds = self.orders

        var1 = pd.Timestamp(1993, 7, 1)
        var2 = pd.Timestamp(1993, 10, 1)

        jn = line_item_ds.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")

        jn = jn[(jn["o_orderdate"] >= var1) & (jn["o_orderdate"] < var2)]
        jn = jn[jn["l_commitdate"] < jn["l_receiptdate"]]

        jn = jn.drop_duplicates(subset=["o_orderpriority", "l_orderkey"])

        gb = jn.groupby("o_orderpriority", as_index=False)
        agg = gb.agg(order_count=pd.NamedAgg(column="o_orderkey", aggfunc="count"))

        result_df = agg.sort_values(["o_orderpriority"])
        
    
    def tpc_q6(self):
        line_item_ds = self.lineitem

        var1 = pd.Timestamp(1994, 1, 1)
        var2 = pd.Timestamp(1995, 1, 1)
        var3 = 0.05
        var4 = 0.07
        var5 = 24

        filt = line_item_ds[
            (line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)
        ]
        filt = filt[(filt["l_discount"] >= var3) & (filt["l_discount"] <= var4)]
        filt = filt[filt["l_quantity"] < var5]
        result_value = (filt["l_extendedprice"] * filt["l_discount"]).sum()
        result_df = pd.DataFrame({"revenue": [result_value]})

    
    def tpc_q11(self, nation_name: str = "GERMANY"):
        nation = self.nation
        supplier = self.supplier
        partsupp = self.partsupp

        # 1) Find the nation key for “GERMANY”
        nation_key = nation.loc[
            nation["n_name"] == nation_name, "n_nationkey"
        ].iloc[0]

        # 2) Filter supplier to that nation
        sup_filt = supplier[supplier["s_nationkey"] == nation_key]

        # 3) Join partsupp ⇄ supplier (inner join on supplier key)
        ps_sup = (
            partsupp
            .merge(
                sup_filt[["s_suppkey"]],
                left_on="ps_suppkey",
                right_on="s_suppkey",
                how="inner"
            )
        )

        # 4) Compute TOTAL_COST = supplycost * availqty
        ps_sup = ps_sup.copy()
        ps_sup["value"] = ps_sup["ps_supplycost"] * ps_sup["ps_availqty"]

        # 5) Aggregate VALUE by part
        total = (
            ps_sup
            .groupby("ps_partkey", as_index=False)
            .agg(value=("value", "sum"))
        )

        # 6) Threshold = 0.0001 × sum of all VALUEs
        threshold = total["value"].sum() * 0.0001

        # 7) Filter and sort
        result = (
            total[total["value"] > threshold]
            .sort_values("value", ascending=False)
            .reset_index(drop=True)
        )



    # def tpc_q14(self):
    #     # 1) Load the full tables
    #     lineitem = self.duckdb_con.execute("SELECT * FROM lineitem").fetchdf()
    #     part = self.duckdb_con.execute("SELECT * FROM part").fetchdf()

    #     # 2) Define the ship‑date range for all of 1994
    #     start = pd.Timestamp("1995-09-01")
    #     end   = pd.Timestamp("1995-10-01")

    #     # 3) Filter lineitem rows by ship date
    #     mask = (lineitem["l_shipdate"] >= start) & (lineitem["l_shipdate"] < end)
    #     li = lineitem.loc[mask].copy()

    #     # 4) Compute per‑row revenue
    #     li["revenue"] = li["l_extendedprice"] * (1 - li["l_discount"])

    #     # 5) Join to the full part table
    #     df = li.merge(
    #         part,
    #         left_on="l_partkey",
    #         right_on="p_partkey",
    #         how="inner"
    #     )

    #     # 6) Identify PROMO items
    #     promo_mask = df["p_type"].str.startswith("PROMO")

    #     # 7) Sum up promo vs. total revenue
    #     promo_rev = df.loc[promo_mask, "revenue"].sum()
    #     total_rev = df["revenue"].sum()

    #     # 8) Compute the percentage
    #     promo_pct = 100.0 * promo_rev / total_rev

    #     print(promo_pct)
    #     return promo_pct



    def test_sample(self):
        customer = self.customer
        sample = customer.sample(5)
        return sample

    def test_drop_duplicates(self):
        supplier = self.supplier
        deduped = supplier.drop_duplicates(subset=["s_nationkey"])
        return deduped

    def test_drop_columns(self):
        part = self.part
        reduced = part.drop(columns=["p_comment", "p_retailprice"])
        head =  reduced.head()

    def test_fillna(self):
        # Work on a copy
        lineitem = self.lineitem.copy()

        # 2) Inject NULLs where discount < 0.05
        lineitem.loc[:, "l_discount_null"] = lineitem["l_discount"].mask(
            lineitem["l_discount"] < 0.05,
            other=pd.NA
        )

        # 3) Compute the mean of the “null‑augmented” series
        mean_discount = lineitem["l_discount_null"].mean()

        # 4) Fill the NULLs with that mean
        lineitem["l_discount_null"] = lineitem["l_discount_null"].fillna(mean_discount)

        return lineitem

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



# con = duckdb.connect('tpch.duckdb')
# p = PandaTester(con)
# p.tpc_q1()
# p.tpc_q4()
# p.tpc_q6()
# p.tpc_q11()
# p.test_drop_columns()
# p.test_drop_duplicates()
# p.test_dropna()
# p.test_fillna()
# p.test_get_dummies()
# p.test_groupby_agg()
# p.test_isna_sum()
# p.test_sample()
