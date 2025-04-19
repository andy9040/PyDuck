"""
Abstract class to define all the testing methods
"""

class Framework_tester:

    def __init__(self, con):
        self.duckdb_con = con

    @abstractmethod
    def test_something(self):
        pass

    @abstractmethod
    def test_something2(self):
        pass



#class Pandas_tester(Framework_tester):

    #def test_something(self):
        # actually implement test_something

