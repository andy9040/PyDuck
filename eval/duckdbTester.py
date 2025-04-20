from framework_tester import FrameworkTester
import duckdb
import pandas as pd

class Duckdb_tester(FrameworkTester):

    def tpc_q1(self):
        result_df = self.duckdb_con.execute("PRAGMA tpch(1);").fetchdf()
        print(result_df)
        return

    
    def tpc_q3(self):
        result_df = self.duckdb_con.execute("PRAGMA tpch(3);").fetchdf()
        print(result_df)
        return

if __name__ == '__main__':
    con = duckdb.connect('tpch.duckdb')
    d = Duckdb_tester(con)
    d.tpc_q1()
    d.tpc_q3()