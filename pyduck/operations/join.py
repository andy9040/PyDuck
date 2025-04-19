def apply_join(left_query, params, table_name=None, conn=None):
    right_quack = params["other"]
    on = params.get("on")
    how = params.get("how", "left").upper()
    lsuffix = params.get("lsuffix", "")
    rsuffix = params.get("rsuffix", "")

    if not conn:
        raise ValueError("conn is required to resolve schema")

    left_alias = "l"
    right_alias = "r"

    # Compile both subqueries
    left_sql = f"({left_query}) AS {left_alias}"
    right_sql = f"({right_quack.to_sql()}) AS {right_alias}"

    # Determine join columns
    if on is None:
        raise ValueError("You must specify 'on' for the join.")
    if isinstance(on, str):
        on = [on]

    join_condition = " AND ".join(
        f"{left_alias}.{col} = {right_alias}.{col}" for col in on
    )

    # Get column names
    left_cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    right_cols = conn.execute(f"PRAGMA table_info({right_quack.table_name})").fetchall()

    left_col_names = [col[1] for col in left_cols]
    right_col_names = [col[1] for col in right_cols]

    # Remove duplicate join columns from right if present in left
    select_clause = []

    for col in left_col_names:
        colname = f"{col}{lsuffix}" if col in right_col_names else col
        select_clause.append(f"{left_alias}.{col} AS {colname}")

    for col in right_col_names:
        if col in left_col_names and col not in on:
            colname = f"{col}{rsuffix}"
            select_clause.append(f"{right_alias}.{col} AS {colname}")
        elif col not in left_col_names:
            select_clause.append(f"{right_alias}.{col}")

    select_expr = ", ".join(select_clause)

    return f"""
    SELECT {select_expr}
    FROM {left_sql}
    {how} JOIN {right_sql}
    ON {join_condition}
    """
