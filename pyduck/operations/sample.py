def apply_sample(query, params, table_name=None, conn=None):
    import random

    n = params.get("n")
    frac = params.get("frac")
    replace = params.get("replace", False)
    random_state = params.get("random_state")

    if conn is None:
        raise ValueError("conn is required for sampling")
    if n is None and frac is None:
        raise ValueError("Must specify either n or frac")
    if n is not None and frac is not None:
        raise ValueError("Cannot specify both n and frac")

    # Estimate row count if using frac
    if frac is not None:
        count_query = f"SELECT COUNT(*) FROM ({query}) AS base"
        total_rows = conn.execute(count_query).fetchone()[0]
        n = max(1, int(frac * total_rows))

    if replace:
        # With replacement: join random values repeatedly, order by random()
        return f"""
        WITH base AS (
            SELECT * FROM ({query}) AS original
        ),
        expanded AS (
            SELECT base.*
            FROM base, range({n})
        )
        SELECT * FROM expanded
        ORDER BY RANDOM()
        LIMIT {n}
        """
    else:
        # Without replacement: shuffle with seed using random()
        return f"""
        SELECT * FROM ({query}) AS sub
        ORDER BY RANDOM()
        LIMIT {n}
        """
