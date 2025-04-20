import duckdb
from pandaTester import PandaTester
from pyduckTester import PyDuckTester
from duckdbTester import DuckTester


def clean_up():
    con.execute("DROP TABLE IF EXISTS customer;")
    con.execute("DROP TABLE IF EXISTS lineitem;")
    con.execute("DROP TABLE IF EXISTS nation;")
    con.execute("DROP TABLE IF EXISTS orders;")
    con.execute("DROP TABLE IF EXISTS part;")
    con.execute("DROP TABLE IF EXISTS partsupp;")
    con.execute("DROP TABLE IF EXISTS region;")
    con.execute("DROP TABLE IF EXISTS supplier;")

con = duckdb.connect('tpch.duckdb')
con.execute("INSTALL tpch;")
con.execute("LOAD tpch;")
testers = [PandaTester(con), PyDuckTester(con), DuckTester(con)]


scaling_factors = [0.001]
for scale in scaling_factors:
    clean_up()

    #generate  tpc-h with scale factor
    con.execute(f"CALL dbgen(sf={scale});")

    for tester in testers:
        tester.test_all()

