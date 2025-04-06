from .filter import apply_filter
from .assign import apply_assign
from .groupby import apply_groupby, apply_agg

def apply_operation(query, op, val):
    if op == "filter":
        return apply_filter(query, val)
    elif op == "assign":
        return apply_assign(query, val)
    elif op == "groupby":
        return apply_groupby(query, val)
    elif op == "agg":
        return apply_agg(query, val)
    else:
        raise ValueError(f"Unsupported operation: {op}")
