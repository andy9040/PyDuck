import re

def apply_isna(query, columns, table_name=None, conn=None):
    """
    Replace the SELECT clause with one that returns booleans for NULL values.
    
    If the query uses SELECT *, then we dynamically retrieve the column names via DuckDB.
    If `columns` is provided (as a list), only those columns are processed; otherwise, process all columns.
    """
    # Check if query is in form "SELECT * ..."
    # star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
    # if star_match:
        #rest_of_query = star_match.group(1)
    if conn is None or table_name is None:
        raise ValueError("Connection and table name are required to expand SELECT *")
        
    # Get schema dynamically from the table using DuckDB's PRAGMA
    col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    # The column name is typically at index 1
    schema = [row[1] for row in col_info]
    #else:
        # for simplicity we'll assume SELECT *, # If not * (i.e. already a fully expanded SELECT clause), you could attempt to parse it,
    #    raise NotImplementedError("apply_isna only supports queries that begin with SELECT * for now.")

    # If specific columns are provided, limit the schema to those; otherwise, process all columns.
    if columns is not None:
        schema = [col for col in schema if col in columns]
    
    # Build the SELECT clause: for each column, create a boolean expression
    select_expressions = [
        f"CASE WHEN {col} IS NULL THEN TRUE ELSE FALSE END AS {col}"
        for col in schema
    ]
    new_select = ", ".join(select_expressions)
    
    # Return a new query that wraps the original as a subquery
    return f"SELECT {new_select} FROM ({query}) AS subquery"

    # SELECT <boolean expressions for each column> FROM (SELECT * FROM people) AS subquery
