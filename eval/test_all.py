import duckdb


def benchmark():
    """
    Calculate time to complete query and memory consumption
    """
    pass

"""
Looping through a list of different scaling factors

    Generate a database in duckdb of that scaling factor

    benhcmark each query for duckdb, pyduck, and pandas, with the benchmark function on the database of the scaling factor
"""


scaling_factors = [0.05, 0.5, 5]

for scale in scaling_factors:
    con = duckdb.connect('tpch.duckdb')

    # Load the TPC-H extension
    con.execute("INSTALL tpch;")
    con.execute("LOAD tpch;")

    con.execute(f"CALL dbgen(sf={scale});")

    # WE NEED AN ABSTRACT TESTER CLASS THAT ALL TESTERS INHERIT FROM

    ducdkdb_tester = Duckdb_tester()
    pyduck_tester = Pyduck_tester()
    pandas_tester = Pandas_tester()

    objects = [ducdkdb_tester, pyduck_tester, pandas_tester]

    # Get all method names of the class (excluding dunder methods)
    method_names = [
        name for name in dir(MyClass)
        if callable(getattr(MyClass, name)) and not name.startswith("__")
    ]

    # Run benchmark on each method of each object
    for obj in objects:
        for method_name in method_names:
            method = getattr(obj, method_name)
            benchmark(method)

    # write the result to csv




