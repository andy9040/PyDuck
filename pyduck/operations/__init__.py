from .filter import apply_filter
from .assign import apply_assign
from .groupby import apply_groupby, apply_agg
from .select import apply_limit_offset, apply_select

def apply_operation(query, op, val):
    if op == "filter":
        return apply_filter(query, val)
    elif op == "assign":
        return apply_assign(query, val)
    elif op == "groupby":
        return apply_groupby(query, val)
    elif op == "agg":
        return apply_agg(query, val)
    elif op == "select":
        return apply_select(query, val)
    elif op == "limit_offset":
        return apply_limit_offset(query, val)
    else:
        raise ValueError(f"Unsupported operation: {op}")
