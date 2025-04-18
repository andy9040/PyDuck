import re

def apply_fillna(query, fill_value, table_name=None, conn=None):
    """
    Modify the provided SQL query to replace NULL values using COALESCE.
    Supports scalar, per-column, or aggregate (mean/median) fills.
    """

    if conn is None or table_name is None:
        raise ValueError("Both connection and table name are required for fillna.")

    # Retrieve schema for fallback/default column names
    col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    schema = [row[1] for row in col_info]

    # Helper: format scalar values (string literals quoted)
    def fmt_val(val):
        return f"'{val}'" if isinstance(val, str) else str(val)


    def parse_select_clause(select_clause):
        columns = []
        current = ''
        depth = 0
        for char in select_clause:
            if char == ',' and depth == 0:
                columns.append(current.strip())
                current = ''
            else:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                current += char
        if current:
            columns.append(current.strip())

        result = []
        for col_expr in columns:
            parts = col_expr.rsplit(" AS ", 1)
            if len(parts) == 2:
                expr, alias = parts[0].strip(), parts[1].strip()
            else:
                expr = alias = col_expr.strip()
            result.append((expr, alias))
        return result

    # Detect SELECT clause type
    star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
    if star_match:
        rest_of_query = star_match.group(1)
        column_expr_pairs = [(col, col) for col in schema]
    else:
        query_cleaned = " ".join(query.strip().split())
        select_match = re.match(r"SELECT\s+(.+?)\s+FROM\s+(.+)", query_cleaned, re.IGNORECASE)
        if not select_match:
            raise NotImplementedError("Could not parse SELECT clause for fillna.")
        select_clause = select_match.group(1)
        rest_of_query = "FROM " + select_match.group(2)
        column_expr_pairs = parse_select_clause(select_clause)

    # Normalize fill_value
    if not isinstance(fill_value, dict):
        fill_value = {col: fill_value for col in schema}

    expressions = []
    for col_expr, alias in column_expr_pairs:
        if alias in fill_value:
            replacement = fill_value[alias]
            if isinstance(replacement, str) and replacement.lower() in ["mean", "median"]:
                agg_func = "AVG" if replacement.lower() == "mean" else "MEDIAN"
                expr = f"COALESCE({col_expr}, (SELECT {agg_func}({alias}) FROM ({query}) AS agg_sub)) AS {alias}"
            else:
                expr = f"COALESCE({col_expr}, {fmt_val(replacement)}) AS {alias}"
        else:
            expr = f"{col_expr} AS {alias}"
        expressions.append(expr)

    return f"SELECT {', '.join(expressions)} {rest_of_query}"


# import re

# def apply_fillna(query, fill_value, table_name=None, conn=None):
#     """
#     Modify the provided SQL query to replace NULL values using COALESCE.
#     Supports scalar, per-column, or aggregate (mean/median) fills.
#     """

#     if conn is None or table_name is None:
#         raise ValueError("Both connection and table name are required for fillna.")

#     # Always retrieve schema first
#     col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
#     schema = [row[1] for row in col_info]

#     # Try to match SELECT * — fallback to generic parsing
#     star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
#     if star_match:
#         rest_of_query = star_match.group(1)
#         base_columns = schema
#     else:
#         # fallback: assume this is a full SELECT — parse column list manually
#         query_cleaned = " ".join(query.strip().split())

#         select_match = re.match(r"SELECT\s+(.+?)\s+FROM\s+(.+)", query_cleaned, re.IGNORECASE)
#         if not select_match:
#             raise NotImplementedError("Could not parse SELECT clause for fillna.")
#         # select_match = re.match(r"SELECT\s+(.+?)\s+FROM\s+(.+)", query, re.IGNORECASE | re.DOTALL)
#         # if not select_match:
#         #     raise NotImplementedError("Could not parse SELECT clause for fillna.")
        
#         select_clause = select_match.group(1)
#         rest_of_query = "FROM " + select_match.group(2)
#         base_columns = [col.strip().split(" AS ")[-1] for col in select_clause.split(",")]

#     # Helper: format scalar value (add quotes if string)
#     def fmt_val(val):
#         return f"'{val}'" if isinstance(val, str) else str(val)

#     # If scalar, broadcast to all columns
#     if not isinstance(fill_value, dict):
#         fill_value = {col: fill_value for col in schema}

#     expressions = []
#     for col in base_columns:
#         if col in fill_value:
#             replacement = fill_value[col]
#             if isinstance(replacement, str) and replacement.lower() in ["mean", "median"]:
#                 agg_func = "AVG" if replacement.lower() == "mean" else "MEDIAN"
#                 expr = f"COALESCE({col}, (SELECT {agg_func}({col}) FROM ({query}) AS agg_sub)) AS {col}"
#             else:
#                 expr = f"COALESCE({col}, {fmt_val(replacement)}) AS {col}"
#             expressions.append(expr)
#         else:
#             expressions.append(f"{col} AS {col}")

#     return f"SELECT {', '.join(expressions)} {rest_of_query}"

# import re

# def apply_fillna(query, fill_value, table_name=None, conn=None):
#     """
#     Modify the provided SQL query to replace NULL values using COALESCE.
    
#     It supports three common cases:
#       1. fill_value is a scalar (e.g., 0 or 'missing') to fill all NULLs.
#       2. fill_value is a dictionary mapping columns to replacement values.
#          For columns not in the dictionary, the column value is left unchanged.
#       3. If any specified fill value (either scalar or per-column) is "mean" or "median",
#          then the replacement is computed as an aggregate over the base query using a subquery.
         
#     Parameters:
#       query: The current SQL query (assumed to start with SELECT * FROM ...).
#       fill_value: Either a scalar or a dictionary of column-to-fill mapping.
#       table_name: The table name (used with PRAGMA to retrieve schema).
#       conn: A DuckDB connection object.
    
#     Returns:
#       A new SQL query where the SELECT clause is replaced by explicit column expressions using COALESCE.
#     """
#     # Verify the query begins with SELECT *
#     star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
#     if not star_match:
#         raise NotImplementedError("apply_fillna only supports queries starting with SELECT * for now.")
#     rest_of_query = star_match.group(1)
    
#     if conn is None or table_name is None:
#         raise ValueError("Both connection and table name are required for fillna.")
    
#     # Retrieve the schema dynamically via PRAGMA.
#     col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
#     schema = [row[1] for row in col_info] 
    
#     # Helper: format scalar value (add quotes if string)
#     def fmt_val(val):
#         return f"'{val}'" if isinstance(val, str) else str(val)
    
#     # If fill_value is not a dict, turn it into a dict where the fill value is the same for all columns
#     # because we are filling all columns with the same thing
#     if not isinstance(fill_value, dict):
#         fill_value = {col: fill_value for col in schema}

#     expressions = []

#     # fill_value is a dict
#     # For each column in the schema, see if we need to apply a fill.
#     for col in schema:
#         if col in fill_value:
#             replacement = fill_value[col]

#             # If replacement is "mean" or "median", create a subquery to compute that
#             if isinstance(replacement, str) and replacement.lower() in ["mean", "median"]:
#                 agg_func = "AVG" if replacement.lower() == "mean" else "MEDIAN"
#                 # The subquery computes the aggregate over the base query.
#                 expr = f"COALESCE({col}, (SELECT {agg_func}({col}) FROM ({query}) AS agg_sub)) AS {col}"
#             else:
#                 # Otherwise, use the provided scalar (formatted properly)
#                 expr = f"COALESCE({col}, {fmt_val(replacement)}) AS {col}"
#             expressions.append(expr)
#         else:
#             # For columns not specified, simply include the column as-is.
#             expressions.append(col)
    
#     new_select = ", ".join(expressions)
#     # Return a new query with an explicit SELECT clause
#     return f"SELECT {new_select} {rest_of_query}"


# """
# If the fill is a scalar (or a dictionary mapping with a scalar), it does:
# COALESCE(col, <scalar_value>) AS col, COALESCE(col, <scalar_value>) AS col, ...

# If the fill value is specified as "mean" or "median", it generates:
# COALESCE(col, (SELECT AVG(col) FROM (<original_query>) AS agg_sub)) AS col
# COALESCE(col, (SELECT Median(col) FROM (<original_query>) AS agg_sub)) AS col
# """
