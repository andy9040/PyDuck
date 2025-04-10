def apply_assign(query, assignments):
    assign_exprs = [f"{expr} AS {col}" for col, expr in assignments.items()]
    return f"SELECT {', '.join(assign_exprs)}, * FROM ({query}) AS sub"
