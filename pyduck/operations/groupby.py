import re

def apply_groupby(query, cols):
    # Encodes groupby columns as a comment prefix
    return f"--GROUPBY {','.join(cols)}\n{query}"


def apply_agg(query, agg_val, table_name=None, conn=None):
    # Check if the query starts with a GROUPBY context comment
    if query.startswith("--GROUPBY"):
        line, query = query.split("\n", 1)
        groupby_cols = line.replace("--GROUPBY ", "").split(",")

        def quote_identifier(identifier):
            return f'"{identifier}"'

        group_clause = ", ".join(quote_identifier(col) for col in groupby_cols)

        if not isinstance(agg_val, dict):
            raise ValueError("For groupby aggregation, a dictionary mapping is expected.")

        select_parts = []
        for col, funcs in agg_val.items():
            if not isinstance(funcs, list):
                funcs = [funcs]
            for func in funcs:
                select_parts.append(
                    f"{func.upper()}({quote_identifier(col)}) AS {quote_identifier(col + '_' + func)}"
                )

        select_clause = ", ".join(
            [quote_identifier(col) for col in groupby_cols] + select_parts
        )

        return f"SELECT {select_clause} FROM ({query}) AS sub GROUP BY {group_clause}"

    else:
        # Global aggregation (no groupby): must use PRAGMA to get column names
        if conn is None or table_name is None:
            raise ValueError("Connection and table name are required for global aggregation.")

        col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        schema = [row[1] for row in col_info]

        if isinstance(agg_val, list):
            select_exprs = []
            for col in schema:
                for func in agg_val:
                    select_exprs.append(f"{func.upper()}({col}) AS {col}_{func}")
            select_clause = ", ".join(select_exprs)
        else:
            raise ValueError("For global aggregation, agg must be a list.")

        return f"SELECT {select_clause} FROM ({query}) AS subquery"

# import re

# def apply_groupby(query, cols):
#     # Save groupby columns inside a comment in the SQL (or persist elsewhere)
#     # In a real version, you’d use an object, not raw SQL
#     return f"--GROUPBY {','.join(cols)}\n{query}"




# def apply_agg(query, agg_val, table_name=None, conn=None):
#     # Check if query has a groupby context (the "--GROUPBY" comment)
#     if query.startswith("--GROUPBY"):
#         groupby_cols = []
#         line, query = query.split("\n", 1)
#         groupby_cols = line.replace("--GROUPBY ", "").split(",")

#         # Helper function to quote SQL identifiers
#         def quote_identifier(identifier):
#             return f'"{identifier}"'

#         # Build the GROUP BY clause with quoted identifiers
#         group_clause = ", ".join(quote_identifier(col) for col in groupby_cols)
        
#         # Expect agg_val to be a dictionary in a groupby context.
#         if not isinstance(agg_val, dict):
#             raise ValueError("For groupby aggregation, a dictionary mapping is expected.")
            
#         select_clause = ", ".join(
#             [quote_identifier(col) for col in groupby_cols] +
#             [f"{func.upper()}({quote_identifier(col)}) AS {quote_identifier(col + '_' + func)}"
#              for col, func in agg_val.items()]
#         )
        
        
#         return f"SELECT {select_clause} FROM ({query}) GROUP BY {group_clause}"
#     else:
#         # Global aggregation. We assume queries start with SELECT * Use a regex to detect and capture the FROM clause.
#         # You can't do SUM(*) in SQL, you have to do SUM(col1). SUM(col2),... etc
#         # star_match = re.match(r"SELECT\s+\*\s+(FROM\s+.+)", query, re.IGNORECASE)
#         # if not star_match:
#         #     raise ValueError("Global aggregation currently only supports queries starting with SELECT *")
            
#         # rest_of_query = star_match.group(1)
#         # # Retrieve column names dynamically using DuckDB's PRAGMA if SELECT * is used.
#         # if conn is None or table_name is None:
#         #     raise ValueError("Connection and table name are required to expand SELECT * for aggregation.")
        
#         # CAN WE SKIP THIS REGEX FOR SELECT *???, OUR QUERIES ALWAYS START with SELECT* anyways right?
        
#         col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
#         schema = [row[1] for row in col_info]
        
#         # Determine the aggregation functions.
#         # If agg_val is a list, apply each function to all columns.
#         if isinstance(agg_val, list):
#             select_exprs = []
#             for col in schema:
#                 for func in agg_val:
#                     select_exprs.append(f"{func.upper()}({col}) AS {col}_{func}")
#             select_clause = ", ".join(select_exprs)
#         else:
#             raise ValueError("agg must be a list for global aggregation.")
        
#         # Wrap the existing query as a subquery and apply the aggregation.
#         return f"SELECT {select_clause} FROM ({query}) AS subquery"
    
    
#     """
#     Example query for the test case
#     SELECT SUM(name) AS name_sum, SUM(age) AS age_sum, SUM(salary) AS salary_sum 
#     FROM (SELECT <boolean expressions for each column> FROM (SELECT * FROM people) AS subquery) AS subquery
#     """

