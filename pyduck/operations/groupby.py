def apply_groupby(query, cols):
    # Save groupby columns inside a comment in the SQL (or persist elsewhere)
    # In a real version, you’d use an object, not raw SQL
    return f"--GROUPBY {','.join(cols)}\n{query}"

# query: groupy col1
# query 

def apply_agg(query, agg_dict):
    # Assume last groupby was just before this (simplified)
    groupby_cols = []
    if query.startswith("--GROUPBY"):
        line, query = query.split("\n", 1)
        groupby_cols = line.replace("--GROUPBY ", "").split(",")

    group_clause = ", ".join(groupby_cols)
    select_clause = ", ".join(groupby_cols + [f"{func.upper()}({col}) AS {col}_{func}" for col, func in agg_dict.items()])
    return f"SELECT {select_clause} FROM ({query}) GROUP BY {group_clause}"