def apply_drop_columns(query, columns_to_drop, table_name=None, conn=None):
    if conn is None or table_name is None:
        raise ValueError("Both connection and table name are required for drop(columns=...)")

    # Get all column names
    col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    schema = [row[1] for row in col_info]

    # Drop the specified ones
    keep_cols = [col for col in schema if col not in columns_to_drop]
    if not keep_cols:
        raise ValueError("No columns left after dropping.")

    select_clause = ", ".join(keep_cols)
    return f"SELECT {select_clause} FROM ({query}) AS sub"