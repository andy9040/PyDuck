# PyDuck
PyDuck 🦆 is a high-performance Python library for ML data preprocessing, built on DuckDB with a Pandas-like API. It enables fast, multi-threaded, out-of-core processing, handling large datasets efficiently. PyDuck accelerates ML workflows by optimizing queries while ensuring seamless integration with Pandas and other data tools. 🚀



Part 1: Usage Instructions
---------------------------

To install:

    pip install pyduck

To clone the development version:

    git clone https://github.com/your-username/pyduck.git
    cd pyduck
    pip install -e .
    pip install -r requirements.txt

To run the testing suite (from the outer PyDuck directory):

    python3 -m pytest testing/

To get started with basic operations:

```python
from pyduck.quack import Quack
import duckdb

# Connect to DuckDB and load a table
q = Quack("customer", conn=duckdb.connect("tpch.duckdb"))

# Filter, group and aggregate
result = (
    q.filter("c_acctbal > 1000")
     [["c_mktsegment", "c_acctbal"]]
     .groupby("c_mktsegment")
     .agg({"c_acctbal": "mean"})
     .to_df()
)
print(result)

```

For a visual walkthrough of PyDuck’s system architecture and performance benchmarks, refer to the final presentation slides here:
🔗 https://docs.google.com/presentation/d/1SlYmPqAVnjJ9Cac_rlO5bipi_EX7rtRQAzGWK086Duo/edit?usp=sharing



## Part 2: Code Overview
### Core Abstraction: `Quack`
All user operations begin with the `Quack` class in `quack.py`. A Quack is a dataframe-like object the is a chainable, immutable wrapper over a DuckDB table. A Quack can be considered a virtual table.

Each method (e.g., `filter()`, `groupby()`, `agg()`) appends a new operation to an internal list and returns a new Quack object.

Key methods in `quack.py`:
`
- `filter(condition)` – Adds a WHERE clause

- `assign(**kwargs)` – Adds computed columns

- `groupby(cols)` + `agg(dict)` – Performs grouped aggregation

- `fillna(...)`, `dropna(...)`, `isna(...)` – Handles missing values

- `sample(...)` – Random sampling

- `merge(...)` – SQL joins between Quacks

- `to_df()` – Triggers SQL compilation and returns a Pandas DataFrame

- `to_sql()` – Generates SQL via SQLCompiler

- `debug()` – Prints the current operation chain

### SQL Compilation (`compiler.py`)

The `SQLCompiler` class translates the chain of Quack operations into a valid SQL query. It uses `apply_operation(...)` from the `operations/` directory.


```
# compiler.py
from .operations import apply_operation

class SQLCompiler:
    def compile(self):
        for op, val in self.operations:
            query = apply_operation(query, op, val, ...)
        return query
```

### Modular Operations
Each operation is implemented in its own file inside the `operations/` directory:

`filter.py`, `dropna.py`, `fillna.py`, `groupby.py`, etc.

Each contains an `apply()` function that defines how to transform the query string.

### Example:

`filter.py` — applies a WHERE clause

`groupby.py` — injects GROUP BY SQL syntax

`drop_duplicates.py` — applies a ROW_NUMBER() trick

`fillna.py` — uses COALESCE() or CASE depending on value

### Design Philosophy
Lazy execution: nothing runs until `.to_df()` or `.execute()` is called

Chainable & immutable: each operation returns a new `Quack`

SQL transparency: the final SQL is always inspectable via `.to_sql()`

