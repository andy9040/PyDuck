def apply_select(query, columns):
    if query.strip().lower().startswith("select"):
        return query.replace("SELECT *", f"SELECT {', '.join(columns)}")
    return f"SELECT {', '.join(columns)} FROM ({query}) AS sub"

def apply_limit_offset(query, limit_offset):
    limit, offset = limit_offset
    return f"{query} LIMIT {limit} OFFSET {offset}"
