from eval.frameworkTester import FrameworkTester
import pandas as pd

class DuckTester(FrameworkTester):

    def tpc_q1(self):
        result_df = self.duckdb_con.execute("PRAGMA tpch(1);").fetchdf()
        return

    # NOT WORKING FOR PYDUCK
    # def tpc_q3(self):
    #     result_df = self.duckdb_con.execute("PRAGMA tpch(3);").fetchdf()
    #     print(result_df)
    #     return
    
    def tpc_q4(self):
        result_df = self.duckdb_con.execute("PRAGMA tpch(4);").fetchdf()
        

    def tpc_q6(self):
        result_df = self.duckdb_con.execute("PRAGMA tpch(6);").fetchdf()
        
    
    def tpc_q11(self):
        result_df = self.duckdb_con.execute("PRAGMA tpch(11);").fetchdf()
        
    
    def test_sample(self):
        query = "SELECT * FROM customer USING SAMPLE 5 ROWS"
        result = self.duckdb_con.execute(query).fetchdf()

    def test_drop_duplicates(self):
        query = """
        SELECT DISTINCT ON (s_nationkey) * FROM supplier
        """
        result = self.duckdb_con.execute(query).fetchdf()

    def test_drop_columns(self):
        query = """
        SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container
        FROM part
        """
        result = self.duckdb_con.execute(query).fetchdf()

    def test_fillna(self):
        query = """
        WITH base AS (
            SELECT *,
                CASE 
                    WHEN l_discount < 0.05 THEN NULL
                    ELSE l_discount
                END AS l_discount_null
            FROM lineitem
        ),
        stats AS (
            SELECT AVG(l_discount_null) AS mean_val FROM base
        )
        SELECT 
            *,
            COALESCE(l_discount_null, stats.mean_val) AS l_discount_filled
        FROM base, stats
        """
        result = self.duckdb_con.execute(query).fetchdf()


    def test_dropna(self):
        query = "SELECT * FROM orders WHERE o_clerk IS NOT NULL"
        result =  self.duckdb_con.execute(query).fetchdf()

    def test_isna_sum(self):
        query = """
        SELECT
            COUNT(*) - COUNT(o_orderkey) AS o_orderkey_nulls,
            COUNT(*) - COUNT(o_custkey) AS o_custkey_nulls,
            COUNT(*) - COUNT(o_orderstatus) AS o_orderstatus_nulls,
            COUNT(*) - COUNT(o_totalprice) AS o_totalprice_nulls,
            COUNT(*) - COUNT(o_orderdate) AS o_orderdate_nulls,
            COUNT(*) - COUNT(o_orderpriority) AS o_orderpriority_nulls,
            COUNT(*) - COUNT(o_clerk) AS o_clerk_nulls,
            COUNT(*) - COUNT(o_shippriority) AS o_shippriority_nulls,
            COUNT(*) - COUNT(o_comment) AS o_comment_nulls
        FROM orders
        """
        result =  self.duckdb_con.execute(query).fetchdf()

    def test_get_dummies(self):
        query = """
        SELECT
            c_mktsegment = 'AUTOMOBILE' AS is_automobile,
            c_mktsegment = 'BUILDING' AS is_building,
            c_mktsegment = 'FURNITURE' AS is_furniture,
            c_mktsegment = 'HOUSEHOLD' AS is_household,
            c_mktsegment = 'MACHINERY' AS is_machinery
        FROM customer
        """
        result =  self.duckdb_con.execute(query).fetchdf()

    def test_groupby_agg(self):
        query = """
        SELECT 
            l_returnflag,
            SUM(l_quantity) AS total_quantity,
            AVG(l_extendedprice) AS avg_price,
            COUNT(DISTINCT l_orderkey) AS order_count
        FROM lineitem
        GROUP BY l_returnflag
        """
        result = self.duckdb_con.execute(query).fetchdf()    
   

# if __name__ == '__main__':
#     con = duckdb.connect('tpch.duckdb')
#     d = Duckdb_tester(con)
#     d.tpc_q1()
#     d.tpc_q4()
#     d.tpc_q6()
#     d.tpc_q11()