import re

def apply_fillna(query, fill_value, table_name=None, conn=None):
    """
    Modify the provided SQL query to replace NULL values using COALESCE.
    Supports scalar, per-column, or aggregate (mean/median) fills.
    """
    if conn is None or table_name is None:
        raise ValueError("Both connection and table name are required for fillna.")

    # Retrieve base schema for fallback/default column names
    col_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    base_schema = [row[1] for row in col_info]

    # Helper: format scalar values (string literals quoted)
    def fmt_val(val):
        return f"'{val}'" if isinstance(val, str) else str(val)

    # Parse SELECT clause into list of (expr, alias)
    def parse_select_clause(select_clause):
        cols, curr, depth = [], '', 0
        for ch in select_clause:
            if ch == ',' and depth == 0:
                cols.append(curr.strip()); curr = ''
            else:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                curr += ch
        if curr:
            cols.append(curr.strip())
        result = []
        for col_expr in cols:
            parts = col_expr.rsplit(' AS ', 1)
            if len(parts) == 2:
                expr, alias = parts[0].strip(), parts[1].strip()
            else:
                expr = alias = col_expr.strip()
            result.append((expr, alias))
        return result

    # Extract SELECT clause and FROM remainder
    qclean = ' '.join(query.strip().split())
    m = re.match(r"SELECT\s+(.+?)\s+FROM\s+(.+)", qclean, re.IGNORECASE)
    if not m:
        raise NotImplementedError("Could not parse SELECT clause for fillna.")
    select_clause, from_part = m.group(1), m.group(2)
    rest = f"FROM {from_part}"

    # Initial expr/alias pairs
    column_expr_pairs = parse_select_clause(select_clause)

    # Detect and expand any '*' alias
    assign_pairs = []
    has_star = False
    for expr, alias in column_expr_pairs:
        if alias == '*':
            has_star = True
        else:
            assign_pairs.append((expr, alias))
    if has_star:
        # preserve assigned expressions, then all base columns not already assigned
        assigned_aliases = {alias for _, alias in assign_pairs}
        expanded = [] + assign_pairs
        for col in base_schema:
            if col not in assigned_aliases:
                expanded.append((col, col))
        column_expr_pairs = expanded

    # Normalize fill_value to dict mapping aliases
    if not isinstance(fill_value, dict):
        fill_value = {alias: fill_value for _, alias in column_expr_pairs}

    # Build COALESCE expressions
    expressions = []
    for expr, alias in column_expr_pairs:
        if alias in fill_value:
            repl = fill_value[alias]
            if isinstance(repl, str) and repl.lower() in ('mean', 'median'):
                agg_fn = 'AVG' if repl.lower() == 'mean' else 'MEDIAN'
                expressions.append(
                    f"COALESCE({expr}, (SELECT {agg_fn}({alias}) FROM ({query}) AS _fill)) AS {alias}"
                )
            else:
                expressions.append(f"COALESCE({expr}, {fmt_val(repl)}) AS {alias}")
        else:
            expressions.append(f"{expr} AS {alias}")

    return f"SELECT {', '.join(expressions)} {rest}"

