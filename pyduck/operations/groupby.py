import re

def apply_groupby(query, cols):
    # Encodes groupby columns as a comment prefix
    return f"--GROUPBY {','.join(cols)}\n{query}"


def apply_agg(query, agg_val, table_name=None, conn=None):
    # Check if the query starts with a GROUPBY context comment

    def quote_identifier(identifier):
            return f'"{identifier}"'
    
    if query.startswith("--GROUPBY"):
        line, query = query.split("\n", 1)
        groupby_cols = line.replace("--GROUPBY ", "").split(",")

        # def quote_identifier(identifier):
        #     return f'"{identifier}"'

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
        if conn is None or table_name is None:
            raise ValueError("Connection and table_name are required for global aggregation.")

        # Fetch column names from DuckDB schema
        col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        all_cols = [row[1] for row in col_info]

        select_parts = []

        # Dict mapping: only aggregate specified columns
        if isinstance(agg_val, dict):
            for col, funcs in agg_val.items():
                if not isinstance(funcs, list):
                    funcs = [funcs]
                for func in funcs:
                    func_upper = func.upper()
                    alias = f"{col}_{func}"
                    select_parts.append(
                        f"{func_upper}({quote_identifier(col)}) AS {quote_identifier(alias)}"
                    )
        # List of functions: apply to every column
        elif isinstance(agg_val, list):
            for col in all_cols:
                for func in agg_val:
                    func_upper = func.upper()
                    alias = f"{col}_{func}"
                    select_parts.append(
                        f"{func_upper}({quote_identifier(col)}) AS {quote_identifier(alias)}"
                    )
        else:
            raise ValueError("For global aggregation, agg_val must be a list or a dict.")

        select_clause = ", ".join(select_parts)
        return f"SELECT {select_clause} FROM ({query}) AS subquery"
        



