# import re

def apply_dropna_rows(query, dummy, table_name=None, conn=None):
    """
    Modify a query that starts with SELECT * to add a filter so that rows with any NULL values are dropped.
    """
    # Match "SELECT * FROM ...". If not, we raise since our current
    # implementation assumes SELECT *.
    # star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
    # if not star_match:
        # raise NotImplementedError("apply_dropna_rows only supports queries starting with SELECT *")
    # rest_of_query = star_match.group(1)
    if conn is None or table_name is None:
        raise ValueError("Connection and table name are required to expand SELECT * for dropna_rows")
    
    # Dynamically retrieve column names.
    col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    schema = [row[1] for row in col_info]
    
    # Build a WHERE clause that requires all columns to be not null.
    conditions = [f"{col} IS NOT NULL" for col in schema]
    where_clause = " AND ".join(conditions)
    
    # Wrap the original query as a subquery and filter rows.
    return f"SELECT * FROM ({query}) AS subquery WHERE {where_clause}"

def apply_dropna_columns(query, dummy, table_name=None, conn=None):
    """
    Modify the provided SQL query to drop columns (axis=1) that contain any NULL values.
    
    For each column, it executes a subquery that computes the number of NULLs. Only columns with a count of zero are kept.
    
    Parameters:
      query: The current SQL query (assumed to begin with SELECT * FROM ...).
      dummy: Unused value
      table_name: The original table name to use with PRAGMA table_info.
      conn: A DuckDB connection object.
    
    Returns:
      A modified SQL query with an explicit SELECT list that only includes columns with no NULL values.
    """
    # Ensure the query uses the SELECT * pattern.
    # star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
    # if not star_match:
        # raise NotImplementedError("apply_dropna_columns only supports queries starting with SELECT *")
    # rest_of_query = star_match.group(1)
    
    # Check for required parameters.
    if conn is None or table_name is None:
        raise ValueError("Both connection and table name are required for dropna_columns.")
    
    # Retrieve column names dynamically via PRAGMA table_info.
    col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    schema = [row[1] for row in col_info]
    
    # Build one combined query that counts nulls per column.
    expr_list = [
        f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count_{col}" 
        for col in schema
    ]
    combined_expr = ", ".join(expr_list)
    count_query = f"SELECT {combined_expr} FROM ({query}) AS subq"
    
    # Execute the combined null count query once, count column nulls all at once
    null_counts = conn.execute(count_query).fetchone()
    
    # Determine which columns have no null values.
    keep_cols = [col for col, count in zip(schema, null_counts) if count == 0]
    
    if not keep_cols:
        raise ValueError("No columns left after dropna(axis=1).")
    
    # Build an explicit SELECT clause using only the columns to keep.
    select_clause = ", ".join(keep_cols)
    # Rebuild the query: the inner query is still your original query,
    # and the outer query selects only the columns with no NULLs.
    return f"SELECT {select_clause} FROM ({query}) AS subq"
