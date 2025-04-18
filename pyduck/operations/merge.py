def apply_merge(left_query, params, table_name=None, conn=None):
    right = params["right"]
    how = params["how"].upper()
    on = params.get("on")
    left_on = params.get("left_on")
    right_on = params.get("right_on")
    suffixes = params.get("suffixes", ("_x", "_y"))

    left_alias = "l"
    right_alias = "r"

    left_sql = f"({left_query}) AS {left_alias}"
    right_sql = f"({right.to_sql()}) AS {right_alias}"

    # Normalize join keys
    if on:
        if isinstance(on, str):
            on = [on]
        join_cond = " AND ".join(f"{left_alias}.{col} = {right_alias}.{col}" for col in on)
    elif left_on and right_on:
        if isinstance(left_on, str):
            left_on = [left_on]
        if isinstance(right_on, str):
            right_on = [right_on]
        assert len(left_on) == len(right_on)
        join_cond = " AND ".join(
            f"{left_alias}.{l} = {right_alias}.{r}" for l, r in zip(left_on, right_on)
        )
        on = []  # ensure 'on' is iterable even if unused
    else:
        raise ValueError("Must specify either 'on' or both 'left_on' and 'right_on'")

    # Fetch schemas
    left_cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    right_cols = conn.execute(f"PRAGMA table_info({right.table_name})").fetchall()
    left_colnames = [col[1] for col in left_cols]
    right_colnames = [col[1] for col in right_cols]

    # Build SELECT clause with suffixing
    select_expr = []
    for col in left_colnames:
        if col in on:
            # For outer/left/right joins: use COALESCE to keep non-null key
            col_expr = f"COALESCE({left_alias}.{col}, {right_alias}.{col})"
            select_expr.append(f"{col_expr} AS {col}")
        else:
            col_name = f"{col}{suffixes[0]}" if col in right_colnames else col
            select_expr.append(f"{left_alias}.{col} AS {col_name}")
    # for col in left_colnames:
    #     col_name = f"{col}{suffixes[0]}" if col in right_colnames and col not in on else col
    #     select_expr.append(f"{left_alias}.{col} AS {col_name}")

    for col in right_colnames:
        # Skip join key only if 'on' is used (same col on both sides)
        if on and col in on:
            continue
        col_name = f"{col}{suffixes[1]}" if col in left_colnames else col
        select_expr.append(f"{right_alias}.{col} AS {col_name}")

    # for col in right_colnames:
    #     if (on and col in on) or (right_on and col in right_on):
    #         continue  # skip join keys
    #     col_name = f"{col}{suffixes[1]}" if col in left_colnames else col
    #     select_expr.append(f"{right_alias}.{col} AS {col_name}")

    return f"""
    SELECT {', '.join(select_expr)}
    FROM {left_sql}
    {how} JOIN {right_sql}
    ON {join_cond}
    """
