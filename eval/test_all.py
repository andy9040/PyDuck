import duckdb
from pandaTester import PandaTester
from pyduckTester import PyDuckTester
from duckdbTester import DuckTester
import os
import gc

def clean_up():
    con.execute("DROP TABLE IF EXISTS customer;")
    con.execute("DROP TABLE IF EXISTS lineitem;")
    con.execute("DROP TABLE IF EXISTS nation;")
    con.execute("DROP TABLE IF EXISTS orders;")
    con.execute("DROP TABLE IF EXISTS part;")
    con.execute("DROP TABLE IF EXISTS partsupp;")
    con.execute("DROP TABLE IF EXISTS region;")
    con.execute("DROP TABLE IF EXISTS supplier;")


for filename in ["Duckdb_Results.csv", "Pandas_Results.csv", "Pyduck_Results.csv"]:
    try:
        os.remove(filename)
        print(f"Removed old result file: {filename}")
    except FileNotFoundError:
        pass  # If the file doesn't exist, ignore

con = duckdb.connect('tpch.duckdb')
con.execute("INSTALL tpch;")
con.execute("LOAD tpch;")


scaling_factors = [0.005, 0.01, .1, 1, 2, 4, 8, 10]
num_of_trials = 10

#* Testing Pandas across all scaling
for scale in scaling_factors:
    clean_up()
    #generate  tpc-h with scale factor
    con.execute(f"CALL dbgen(sf={scale});")
    tester = PandaTester(con)
    
    
    for i in range(0, num_of_trials):
        tester.test_all(scale)
        
        # Free up memory
        del tester
        gc.collect()

        # Recreate the tester for the next trial (if needed)
        tester = PandaTester(con)

# #* Testing Pyduck across all scaling
for scale in scaling_factors:
    clean_up()
    #generate  tpc-h with scale factor
    con.execute(f"CALL dbgen(sf={scale});")
    tester = PyDuckTester(con)
    
    
    for i in range(0, num_of_trials):
        tester.test_all(scale)
        
        # Free up memory
        del tester
        gc.collect()

        tester = PyDuckTester(con)

#* Testing Duckdb across all scaling
for scale in scaling_factors:
    clean_up()
    #generate  tpc-h with scale factor
    con.execute(f"CALL dbgen(sf={scale});")
    tester = DuckTester(con)
    

    for i in range(0, num_of_trials):
        tester.test_all(scale)

        # Free up memory
        del tester
        gc.collect()

        tester = DuckTester(con)