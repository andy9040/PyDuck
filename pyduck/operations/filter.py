def apply_filter(query, condition):
    return f"SELECT * FROM ({query}) WHERE {condition}"
