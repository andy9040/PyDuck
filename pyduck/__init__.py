# exposes main API

from .quack import Quack
from .io import _read_table, _from_dataframe, _from_csv

# Mimic pandas
def from_df(df, name="df", conn=None):
    return _from_dataframe(df, name=name, conn=conn)

def from_csv(filepath, name="csv_table", conn=None, **read_options):
    return _from_csv(filepath, name=name, conn=conn, **read_options)

def from_table(name, conn=None):
    return _read_table(name, conn)

__all__ = ["Quack", "from_df", "from_csv", "from_table"]
