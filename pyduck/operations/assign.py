def apply_assign(query, assignments):
    assign_exprs = ", ".join([f"{expr} AS {col}" for col, expr in assignments.items()])
    return f"SELECT *, {assign_exprs} FROM ({query})"
