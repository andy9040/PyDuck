"""
Abstract class to define all the testing methods
"""
from abc import ABC, abstractmethod

class FrameworkTester:

    def __init__(self, con):
        self.duckdb_con = con

    @abstractmethod
    def tpc_q1(self):
        pass

    @abstractmethod
    def tpc_q3(self):
        pass

    @abstractmethod
    def tpc_q4(self):
        pass

    @ abstractmethod
    def tpc_q6(self):
        pass

    @ abstractmethod
    def tpc_q11(self):
        pass

    @abstractmethod
    def tpc_q14(self):
        pass

    @abstractmethod
    def test_something2(self):
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

#class Pandas_tester(Framework_tester):

    #def test_something(self):
        # actually implement test_something

