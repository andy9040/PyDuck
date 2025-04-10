import duckdb
import pandas as pd
import pyduck
from pyduck import Quack

data = {
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "age": [25, 35, 45, 55],
    "salary": [50000, 60000, 70000, 80000]
}

def setup_duck():
    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    return Quack("people", conn=conn)

def test_basic_to_df():
    dp = setup_duck()
    df = dp.to_df()
    assert len(df) == 4
    assert "name" in df.columns

def test_filter():
    dp = setup_duck()
    result = dp.filter("age > 30").to_df()
    assert result.shape[0] == 3
    assert all(result["age"] > 30)

def test_assign():
    dp = setup_duck()
    result = dp.assign(senior="age >= 50").to_df()
    assert "senior" in result.columns
    assert result[result["name"] == "Diana"]["senior"].values[0] == True

def test_getitem_column_select():
    dp = setup_duck()
    result = dp[["name", "salary"]].to_df()
    assert list(result.columns) == ["name", "salary"]
    assert result.shape == (4, 2)

def test_getitem_slice_rows():
    dp = setup_duck()
    result = dp[1:3].to_df()
    assert result.shape[0] == 2
    assert result.iloc[0]["name"] == "Bob"
    assert result.iloc[1]["name"] == "Charlie"

def test_setitem_overwrite_column():
    dp = setup_duck()
    dp["age"] = "age + 10"
    result = dp.to_df()
    assert "age" in result.columns
    assert list(result["age"]) == [35, 45, 55, 65]

def test_get_dummies():
    dp = setup_duck()
    dp.get_dummies("name", ["Alice", "Bob"])
    df = dp.to_df()
    assert "name_Alice" in df.columns
    assert "name_Bob" in df.columns
    assert df[df["name"] == "Alice"]["name_Alice"].values[0] == 1
    assert df[df["name"] == "Bob"]["name_Bob"].values[0] == 1



# def test_groupby_agg():
#     conn = duckdb.connect()
#     df = pd.DataFrame({
#         "group": ["A", "A", "B", "B"],
#         "value": [10, 20, 30, 40]
#     })
#     conn.register("test_table", df)
#     dp = Quack("test_table", conn=conn)
#     result = dp.groupby("group").agg({"value": "sum"}).to_df()
#     assert result.shape[0] == 2
#     assert "value_sum" in result.columns
#     assert result[result["group"] == "A"]["value_sum"].values[0] == 30

def test_to_sql_output():
    dp = setup_duck().filter("age > 40").assign(retired="age > 60")
    sql = dp.to_sql()
    assert "WHERE age > 40" in sql
    assert "retired" in sql

