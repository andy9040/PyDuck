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
        print(result)
        return   

    # NOT WORKING
    # def tpc_q3(self):
    #     var1 = "BUILDING"
    #     var2 = "1995-03-15"  # DuckDB will parse this as a DATE literal

    #     # 1) Push each filter into its own table
    #     cust_q = (
    #         Quack("customer", conn=self.duckdb_con)
    #         .filter(f"c_mktsegment = '{var1}'")
    #     )
    #     ord_q = (
    #         Quack("orders", conn=self.duckdb_con)
    #         .filter(f"o_orderdate < DATE '{var2}'")
    #     )
    #     li_q = (
    #         Quack("lineitem", conn=self.duckdb_con)
    #         .filter(f"l_shipdate > DATE '{var2}'")
    #     )

    #     # 2) Chain your joins
    #     joined = (
    #         cust_q
    #         .merge(ord_q,  left_on="c_custkey", right_on="o_custkey")
    #         .merge(li_q,   left_on="o_orderkey", right_on="l_orderkey")
    #     )

    #     # 3) Project *only* the five columns you need going forward:
    #     #    the three o_* for grouping/ordering, plus the two lineitem cols for revenue
    #     projected = joined[[
    #         "o_orderkey",
    #         "o_orderdate",
    #         "o_shippriority",
    #         "l_extendedprice",
    #         "l_discount",
    #     ]]

    #     # 4) Compute revenue
    #     with_rev = projected.assign(
    #         revenue="l_extendedprice * (1 - l_discount)"
    #     )

    #     # 5) Group, aggregate, rename
    #     aggregated = (
    #         with_rev
    #         .groupby(["o_orderkey", "o_orderdate", "o_shippriority"])
    #         .agg({"revenue": "sum"})
    #         .rename({"revenue_sum": "revenue"})
    #     )

    #     # 6) Sort and take top‑10
    #     result_df = (
    #         aggregated
    #         .sort_values(by=["revenue", "o_orderdate"], ascending=[False, True])
    #         .head(10)
    #     )

    #     print(result_df.to_sql())
    #     result_df = result_df.to_df()
    #     print(result_df)
    #     return result_df

    def tpc_q4(self):
        # 1) Define your date bounds
        start_date = "1993-07-01"   
        end_date   = "1993-10-01"

        # 2) Push the lineitem filter into its own scan
        li_q = (
            Quack("lineitem", conn=self.duckdb_con)
            .filter("l_commitdate < l_receiptdate")
        )

        # 3) Push the orders filter into its own scan
        ord_q = (
            Quack("orders", conn=self.duckdb_con)
            .filter(f"o_orderdate >= DATE '{start_date}' AND o_orderdate < DATE '{end_date}'")
        )

        # 4) Join them on the order key
        joined = li_q.merge(
            ord_q,
            left_on="l_orderkey",
            right_on="o_orderkey"
        )

        # 5) Drop duplicate (order‑priority, orderkey) pairs
        deduped = joined.drop_duplicates(subset=["o_orderpriority", "l_orderkey"])

        # 6) Group by priority and count distinct orders
        aggregated = (
            deduped
            .groupby("o_orderpriority")
            .agg({"l_orderkey": "count"})
            .rename({"l_orderkey_count": "order_count"})
        )

        # 7) Sort by priority (ascending) and pull into a Pandas DataFrame
        result_df = (
            aggregated
            .sort_values(by="o_orderpriority")
            .to_df()
        )

        print(result_df)
        return result_df

    def tpc_q6(self):
        # 1) Parameters (DuckDB will parse these DATE literals and numerics)
        start_date = "1994-01-01"
        end_date   = "1995-01-01"
        low_disc   = 0.05
        high_disc  = 0.07
        qty_limit  = 24

        # 2) Single scan of lineitem with all filters combined
        li_q = (
            Quack("lineitem", conn=self.duckdb_con)
            .filter(
                f"l_shipdate >= DATE '{start_date}' AND "
                f"l_shipdate < DATE '{end_date}' AND "
                f"l_discount >= {low_disc} AND "
                f"l_discount <= {high_disc} AND "
                f"l_quantity < {qty_limit}"
            )
            .assign(revenue="l_extendedprice * l_discount")
        )

        # 3) Select only the revenue column, sum it, rename the aggregate, and fetch
        # 3) Aggregate *only* the revenue column
        result_df = (
            li_q
            .agg({"revenue": "sum"})                # yields a single-row table with revenue_sum
            .rename({"revenue_sum": "revenue"})     
            .to_df()                                
        )

        print(result_df)
        return result_df
    
    from pyduck import Quack

    def tpc_q11(self, nation_name="GERMANY"):
        # 1) Look up the nation key for the given name
        nation_key = (
            Quack("nation", conn=self.duckdb_con)
            .filter(f"n_name = '{nation_name}'")
            [["n_nationkey", 'n_name']]
            .to_df()
            .iloc[0, 0]
        )

        print(nation_key)

        # 2) Compute the global total for that nation: SUM(ps_supplycost * ps_availqty)
        global_sum_df = (
            Quack("partsupp", conn=self.duckdb_con)
            .merge(
                Quack("supplier", conn=self.duckdb_con)
                .filter(f"s_nationkey = {nation_key}"),
                left_on="ps_suppkey",
                right_on="s_suppkey"
            )
            .assign(value="ps_supplycost * ps_availqty")
            .agg({"value": "sum"})
            .to_df()
        )
        threshold = global_sum_df["value_sum"][0] * 0.0001

        # 3) Run the per‑part aggregation, filter by threshold, round, and sort
        result_df = (
            Quack("partsupp", conn=self.duckdb_con)
            .merge(
                Quack("supplier", conn=self.duckdb_con)
                .filter(f"s_nationkey = {nation_key}"),
                left_on="ps_suppkey",
                right_on="s_suppkey"
            )
            .assign(value="ps_supplycost * ps_availqty")
            .groupby("ps_partkey")
            .agg({"value": "sum"})
            .filter(f"value_sum > {threshold}")                            # this becomes a HAVING
            .assign(value="round(value_sum, 2)")    
            .sort_values(by="value_sum", ascending=False)  
            .to_df()
        )

        print(result_df)
        return result_df

    
    # def tpc_q14(self):
    #     ship_start = "1995-09-01"
    #     promo_like = "PROMO%"

    #     # 1) Scan & filter lineitem, compute revenue & promo_rev
    #     li_q = (
    #         Quack("lineitem", conn=self.duckdb_con)
    #         .filter(
    #             f"l_shipdate >= DATE '{ship_start}' AND "
    #             f"l_shipdate < DATE '{ship_start}' + INTERVAL '1' MONTH"
    #         )
    #         .assign(
    #             revenue = "l_extendedprice * (1 - l_discount)",
    #             promo_rev = f"CASE WHEN p_type LIKE '{promo_like}' THEN l_extendedprice * (1 - l_discount) ELSE 0 END"
    #         )
    #     )

    #     # 2) Join to the full part table
    #     joined = li_q.merge(
    #         Quack("part", conn=self.duckdb_con),
    #         left_on="l_partkey",
    #         right_on="p_partkey"
    #     )

    #     # 3) Project down to only the two measures
    #     measures = joined[["revenue", "promo_rev"]]

    #     # 4) Aggregate exactly those two
    #     agg = (
    #         measures
    #         .agg({"promo_rev": "sum", "revenue": "sum"})
    #     )

    #     # 5) Compute the final percentage and project *only* that
    #     final = (
    #         agg
    #         .assign(promo_revenue="round(promo_rev_sum * 100.0 / revenue_sum, 2)")
    #     )
    #     result_df = final[["promo_revenue"]]

    #     print(result_df.to_sql())
    #     result_df.to_df()
    #     print(result_df)
    #     return result_df

    
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

        # 1) Load lineitem and inject NULLs directly into l_discount
        dp = Quack("lineitem", conn=self.duckdb_con)
        dp["l_discount"] = "CASE WHEN l_discount < 0.05 THEN NULL ELSE l_discount END"

        # 2) Fill remaining NULLs in the same column with its mean
        #    Pass a dict so it only applies to l_discount
        dp_filled = dp.fillna({"l_discount": "mean"})

        # 3) Execute
        df = dp_filled.to_df()
        return df


    
    def debug_fillna(self):
        nation = Quack("nation", self.duckdb_con).to_df()
        print(nation)
        return nation

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
    
if __name__ == '__main__':
    con = duckdb.connect('tpch.duckdb')
    quack = PyDuckTester(con)
    quack.tpc_q1()
    quack.tpc_q4()
    quack.tpc_q6()
    quack.tpc_q11()
    quack.test_fillna()
    # quack.debug_fillna()

