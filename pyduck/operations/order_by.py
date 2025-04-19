# --- operations/order_by.py ---
def apply_order_by(query, val):
    """
    Append ORDER BY clause to query.
    `val` is a list of strings like ["col1 ASC", "col2 DESC"]
    """
    return query + f" ORDER BY {', '.join(val)}"
