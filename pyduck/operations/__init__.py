from .filter import apply_filter
from .assign import apply_assign
from .groupby import apply_groupby, apply_agg
from .select import apply_limit_offset, apply_select
from .rename import apply_rename
from .isna import apply_isna
from .dropna import apply_dropna_rows, apply_dropna_columns
from .fillna import apply_fillna  # add this line
from .drop_columns import apply_drop_columns
from .drop_duplicates import apply_drop_duplicates
from .sample import apply_sample
from .merge import apply_merge
from .order_by import apply_order_by


def apply_operation(query, op, val, **kwargs):
    if op == "filter":
        return apply_filter(query, val)
    elif op == "assign":
        return apply_assign(query, val)
    elif op == "groupby":
        return apply_groupby(query, val)
    elif op == "agg":
        return apply_agg(query, val, **kwargs)
    elif op == "select":
        return apply_select(query, val)
    elif op == "limit_offset":
        return apply_limit_offset(query, val)
    elif op == "rename":
        return apply_rename(query, val, **kwargs)
    elif op == "isna":
        return apply_isna(query, val, **kwargs)
    elif op == "dropna_rows":
        return apply_dropna_rows(query, val, **kwargs)
    elif op == "dropna_columns":
        return apply_dropna_columns(query, val, **kwargs)
    elif op == "fillna":
        return apply_fillna(query, val, **kwargs)
    elif op == "drop_columns":
        return apply_drop_columns(query, val, **kwargs) 
    elif op == "drop_duplicates":
        return apply_drop_duplicates(query, val, **kwargs)
    elif op == "sample":
        return apply_sample(query, val, **kwargs)
    elif op == "merge":
        return apply_merge(query, val, **kwargs)
    elif op == "order_by": 
        return apply_order_by(query, val)

    # elif op == "join":
    #     return apply_join(query, val, **kwargs) 
    else:
        raise ValueError(f"Unsupported operation: {op}")
