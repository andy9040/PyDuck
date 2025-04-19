import re

def apply_rename(query, columns, table_name=None, conn=None):
    """
    Apply column renaming in the SELECT clause.
    Handles both SELECT * and explicit SELECT queries.
    """
    # Try to match SELECT * to simplify (old logic)
    star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
    if star_match:
        if conn is None or table_name is None:
            raise ValueError("conn and table_name are required when using SELECT *")
        
        col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        old_cols = [row[1] for row in col_info]
        
        new_cols = [f"{col} AS {columns[col]}" if col in columns else col for col in old_cols]
        return f"SELECT {', '.join(new_cols)} {star_match.group(1)}"

    # Otherwise, handle general SELECT ... FROM ...
    # Split off the SELECT clause
    select_match = re.match(r"\s*SELECT\s+(.*?)\s+FROM\s+(.*)", query, re.IGNORECASE | re.DOTALL)
    if not select_match:
        raise ValueError("Unsupported SQL structure for renaming.")

    select_clause, rest = select_match.groups()

    # Parse comma-separated select expressions
    expressions = [expr.strip() for expr in select_clause.split(",")]

    # Apply renames where matched
    updated_expressions = []
    for expr in expressions:
        # Handle already aliased columns: col AS alias
        alias_match = re.match(r'(.+?)\s+AS\s+("?[\w]+"?)$', expr, re.IGNORECASE)
        if alias_match:
            base, alias = alias_match.groups()
            alias_clean = alias.strip('"')
            new_alias = columns.get(alias_clean, alias_clean)
            updated_expressions.append(f"{base} AS {new_alias}")
        else:
            # Simple column name
            expr_clean = expr.strip('"')
            new_alias = columns.get(expr_clean, expr_clean)
            if expr_clean != new_alias:
                updated_expressions.append(f"{expr} AS {new_alias}")
            else:
                updated_expressions.append(expr)

    return f"SELECT {', '.join(updated_expressions)} FROM {rest}"

# import re

# def apply_rename(query, columns, table_name=None, conn=None):
#     """
#     Apply column renaming to the SQL query.
    
#     If the query uses a star (SELECT *), it fetches the column names for the table
#     dynamically using the provided connection and table_name.
    
#     Parameters:
#       query: the current SQL query (e.g. "SELECT * FROM people")
#       columns: a dict mapping old column names to new column names (e.g. {"name": "new_name"})
#       table_name: the table name from which to retrieve the schema (needed if SELECT * is used)
#       conn: a DuckDB connection object, used to query the schema.
#     """
#     # Detect if the query uses a * pattern.
#     star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
#     # if star_match:
#     rest_of_query = star_match.group(1)
#     if conn is None or table_name is None:
#         raise ValueError("Connection and table name are required to expand SELECT *")
        
#     # Use DuckDB's PRAGMA to get table schema.
#     # The PRAGMA returns rows where index 1 is the column name.
#     col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
#     schema = [row[1] for row in col_info]
    
#     # Build the select clause: for each column, use an alias if it's in the rename mapping.
#     select_cols = []
#     for col in schema:
#         if col in columns:
#             select_cols.append(f"{col} AS {columns[col]}")
#         else:
#             select_cols.append(col)
            
#     new_select = ", ".join(select_cols)
#     return f"SELECT {new_select} {rest_of_query}"
#     # else:
#     #     # If the query has an explicit select list, you could parse it,
#     #     # but for now don't worry about it.
#     #     raise NotImplementedError("apply_rename only supports queries that begin with SELECT *")
