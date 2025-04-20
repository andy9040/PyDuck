"""
Abstract class to define all the testing methods
"""
from abc import ABC, abstractmethod
import tracemalloc
import time
import inspect

class FrameworkTester (ABC):

    def __init__(self, con):
        self.duckdb_con = con
        self.lineitem = self.duckdb_con.execute("SELECT * FROM lineitem").fetchdf()
        self.customer = self.duckdb_con.execute("SELECT * FROM customer").fetchdf()
        self.supplier = self.duckdb_con.execute("SELECT * FROM supplier").fetchdf()
        self.part = self.duckdb_con.execute("SELECT * FROM part").fetchdf()
        self.nation = self.duckdb_con.execute("SELECT * FROM nation").fetchdf()
        self.orders = self.duckdb_con.execute("SELECT * FROM orders").fetchdf()


    @abstractmethod
    def tpc_q1(self):
        pass

    @abstractmethod
    def tpc_q3(self):
        pass


    @abstractmethod
    def test_sample(self):
        pass

    @abstractmethod
    def test_drop_duplicates(self):
        pass

    @abstractmethod
    def test_drop_columns(self):
        pass

    @abstractmethod
    def test_fillna(self):
        pass

    @abstractmethod
    def test_dropna(self):
        pass

    @abstractmethod
    def test_isna_sum(self):
        pass

    @abstractmethod
    def test_get_dummies(self):
        pass

    @abstractmethod
    def test_groupby_agg(self):
        pass

    def benchmark(self, func, *args, **kwargs):
        """Benchmarks a function's execution time and memory usage."""
        tracemalloc.start()
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"Function '{func.__name__}':")
        print(f"  Time: {duration:.6f} seconds")
        print(f"  Memory: {peak / 1024:.2f} KB\n")
        return result

    def test_all(self):
        """Runs all public instance test methods (excluding private and inherited ones) with benchmarking."""
        print(f"\nRunning all test methods in {self.__class__.__name__}:\n")
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith("_") or name == "benchmark" or name == "test_all":
                continue
            if method.__self__.__class__ != self.__class__:
                continue  # skip inherited methods unless overridden

            print("running: " + name)
            self.benchmark(method)





#class Pandas_tester(Framework_tester):

    #def test_something(self):
        # actually implement test_something

