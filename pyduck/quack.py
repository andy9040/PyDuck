# -------- pyduck/quack.py --------
from .compiler import SQLCompiler

class Quack:
    def __init__(self, table_name, conn=None, operations=None):
        import duckdb
        self.table_name = table_name
        self.conn = conn or duckdb.connect()
        self.operations = operations or []  # list of (operation, args)
    
    def __repr__(self):
        rows = self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT 10").fetchall()
        return f"<DuckPandas table='{self.table_name}'>\nPreview (first 10 rows):\n{rows}"



    def _copy_with(self, new_op):
        # Create a new Quack object with the new operation appended
        return Quack(
            self.table_name,
            conn=self.conn,
            operations=self.operations + [new_op]
        )

    def filter(self, condition):
        """
        Add a WHERE clause to the query.
        """
        return self._copy_with(("filter", condition))

    def assign(self, **kwargs):
        """
        Add new columns to the query using expressions.
        Example: dp.assign(new_col="age + 5")
        """
        return self._copy_with(("assign", kwargs))

    def groupby(self, cols):
        """
        Tag the query with groupby columns (actual SQL is built in agg).
        """
        if isinstance(cols, str):
            cols = [cols]
        return self._copy_with(("groupby", cols))

    def agg(self, agg_dict):
        """
        Perform aggregation functions after a groupby.
        Example: dp.groupby("col").agg({"value": "mean"})
        """
        return self._copy_with(("agg", agg_dict))

    def to_sql(self):
        """
        Compile the chain of operations into a final SQL query.
        """
        return SQLCompiler(self.table_name, self.operations).compile()

    def to_df(self):
        """
        Run the SQL query in DuckDB and return a Pandas DataFrame.
        """
        return self.conn.execute(self.to_sql()).df()
    
    def execute(self):
        """
        Run the SQL query in DuckDB and return a Pandas DataFrame.
        """
        return self.conn.execute(self.to_sql()).fetchall()

    def head(self, n=10):
        """
        Preview the top N rows of the original DuckDB table.
        """
        return self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT {n}").fetchall()


    def debug(self):
        """
        Print the list of accumulated operations.
        """
        print("Operations:", self.operations)
