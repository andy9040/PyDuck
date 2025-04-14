def apply_drop_duplicates(query, subset, table_name=None, conn=None):
    """
    Applies SQL DISTINCT or GROUP BY depending on subset.
    
    Parameters:
        - query (str): The current SQL query.
        - subset (list or str or None): Which columns to consider for uniqueness.
    """
    if subset is None:
        # Use SELECT DISTINCT *
        return f"SELECT DISTINCT * FROM ({query}) AS sub"
    
    if isinstance(subset, str):
        subset = [subset]

    # Keep the first row per subset group
    group_cols = ", ".join(subset)
    return f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {group_cols}) AS rn
            FROM ({query}) AS sub
        ) AS ranked
        WHERE rn = 1
    """