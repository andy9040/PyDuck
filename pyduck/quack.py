# -------- pyduck/quack.py --------
from .compiler import SQLCompiler

class Quack:
    def __init__(self, table_name, conn=None, operations=None):
        import duckdb # type: ignore
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

    def __getitem__(self, key):
        # Case 1: Selecting columns
        if isinstance(key, str):
            return self._copy_with(("select", [key]))
        
        elif isinstance(key, list):
            return self._copy_with(("select", key))
        
        # Case 2: Filtering with a condition string
        elif isinstance(key, Quack):
            # Assume the key is a Quack representing a boolean condition
            # You can refine this if you later support Series-like filters
            raise NotImplementedError("Boolean masking with a Quack object isn't implemented yet.")

        elif isinstance(key, slice):
            start = key.start or 0
            stop = key.stop
            if stop is None:
                raise ValueError("Slice must have a stop value")
            return self._copy_with(("limit_offset", (stop - start, start)))

        else:
            raise TypeError(f"Unsupported key type: {type(key)}")

    def __setitem__(self, key, value):
        # Key is column name, value is expression string
        if not isinstance(key, str):
            raise TypeError("Column name must be a string")
        if not isinstance(value, str):
            raise TypeError("Value must be a SQL expression string")

        # Use your assign logic
        self.operations.append(("assign", {key: value}))


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

    def agg(self, agg_val):
        """
        Perform aggregation functions after a groupby.
        Example: dp.groupby("col").agg({"value": "mean"})
        """
        return self._copy_with(("agg", agg_val))

    def get_dummies(quack, column, values, inplace=True):
        assignments = {
            f"{column}_{val}": f"CASE WHEN {column} = '{val}' THEN 1 ELSE 0 END"
            for val in values
        }
        if inplace:
            quack.operations.append(("assign", assignments))
            return None
        return quack.assign(**assignments)

    def rename(self, columns):
        """
        Rename columns based on a dictionary mapping of old to new names.
        For example, dp.rename({"old_col": "new_col"})
        """
        return self._copy_with(("rename", columns))
    
    def isna(self, columns=None):
        """
        Produces a DataFrame of booleans indicating which values are NULL. 
        If `columns` is provided, only those columns are processed; otherwise, all columns are processed.
        """
        return self._copy_with(("isna", columns))
    
    def sum(self):
        """
        Sum across all rows for each column. intended to go with pd.isna()
        """
        return self.agg(["sum"])
    
    def dropna(self, axis=0):
        """
        Drop missing values along rows (axis=0) or columns (axis=1).
        """
        if axis == 0:
            return self._copy_with(("dropna_rows", None))
        elif axis == 1:
            return self._copy_with(("dropna_columns", None))
        else:
            raise ValueError("axis must be 0 or 1")
    
    def fillna(self, fill_value):
        """
        When fill_value is a scalar, replaces missing values in every column with that scalar.
        When fill_value is a dict, applies per-column replacements.
            - when fill_value is a dict, "mean" or "median" is acceptable
        """
        return self._copy_with(("fillna", fill_value))


    def to_sql(self):
        """
        Compile the chain of operations into a final SQL query.
        """
        #return SQLCompiler(self.table_name, self.operations).compile()
        return SQLCompiler(self.table_name, self.operations, self.conn).compile()

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

    def drop(self, columns=None):
        """
        Drop one or more columns from the table (like pandas drop(columns=[...])).
        """
        if columns is None:
            raise ValueError("You must specify columns to drop")
        if isinstance(columns, str):
            columns = [columns]
        return self._copy_with(("drop_columns", columns))
    
    def drop_duplicates(self, subset=None):
        """
        Drop duplicate rows based on all columns (default) or a subset of columns.
        Mimics pandas.drop_duplicates().
        
        Parameters:
            subset (list[str] or str): Columns to consider for identifying duplicates.
        """
        return self._copy_with(("drop_duplicates", subset))
    
    def sample(self, n=None, frac=None, replace=False, random_state=None):
        """
        Randomly sample rows from the DataFrame.
        
        Parameters:
            n (int): Number of rows to return.
            frac (float): Fraction of rows to return.
            replace (bool): Sample with replacement (True) or not (False).
            random_state (int): Seed for reproducibility.
        """
        if n is None and frac is None:
            raise ValueError("Must specify either n or frac")
        if n is not None and frac is not None:
            raise ValueError("Cannot specify both n and frac")

        return self._copy_with(("sample", {
            "n": n,
            "frac": frac,
            "replace": replace,
            "random_state": random_state
        }))