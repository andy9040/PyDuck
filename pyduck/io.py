def _read_table(table_name, conn=None):
    import duckdb
    from .quack import Quack
    return Quack(table_name, conn)

def _from_dataframe(df, name="df", conn=None):
    """
    Registers a Pandas DataFrame as a DuckDB table.

    Parameters:
    - df (pd.DataFrame): The DataFrame to register.
    - name (str): Name to use for the DuckDB table.
    - conn (duckdb.DuckDBPyConnection): Optional DuckDB connection.

    Returns:
    - Quack: A Quack instance pointing to the registered table.
    """
    import duckdb
    from .quack import Quack

    conn = conn or duckdb.connect()
    conn.register(name, df)  # This creates a virtual DuckDB table

    return Quack(name, conn=conn)

def _from_csv(filepath, name="csv_table", conn=None, **read_options):
    """
    Loads a CSV file using DuckDB and wraps it as a Quack object.

    Parameters:
    - filepath (str): Path to the CSV file
    - name (str): Virtual table name in DuckDB
    - conn (duckdb.DuckDBPyConnection): Optional DuckDB connection
    - **read_options: Options passed to DuckDB's read_csv_auto (e.g., header=True)

    Returns:
    - Quack: A Quack instance ready for chaining
    """
    import duckdb
    from .quack import Quack

    conn = conn or duckdb.connect()

    # Construct DuckDB SQL query to read the CSV
    options = " ".join(f"{k}={repr(v)}" for k, v in read_options.items())
    query = f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_csv_auto('{filepath}'{', ' + options if options else ''})"

    conn.execute(query)

    return Quack(name, conn=conn)

