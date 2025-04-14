import duckdb
import pandas as pd
import pyduck
from pyduck import Quack
import numpy as np

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


def test_groupby_agg():
    conn = duckdb.connect()
    df = pd.DataFrame({
        "name": ["A", "A", "B", "B"],
        "value": [10, 20, 30, 40]
    })
    conn.register("test_table", df)
    dp = Quack("test_table", conn=conn)
    result = dp.groupby("name").agg({"value": "sum"}).to_df()
    assert result.shape[0] == 2
    assert "value_sum" in result.columns
    assert result[result["name"] == "A"]["value_sum"].values[0] == 30

def test_to_sql_output():
    dp = setup_duck().filter("age > 40").assign(retired="age > 60")
    sql = dp.to_sql()
    assert "WHERE age > 40" in sql
    assert "retired" in sql

def test_rename():
    dp = setup_duck()
    dp = dp.rename({"name": "new_name", "age": "new_age"})
    result = dp.to_df()
    # Check that the new column name is present and the old one is not.
    assert "new_name" in result.columns, "Renamed column 'new_name' not found in result."
    assert "name" not in result.columns, "Old column name 'name' still appears in result."
    assert "new_age" in result.columns, "Renamed column 'new_age' not found in result."
    assert "age" not in result.columns, "Old column name 'age' still appears in result."

def test_isna():
    data = {
        "name": ["Alice", None, "Charlie", "Diana"],
        "age": [25, 35, 45, None],
        "salary": [50000, 60000, 70000, 80000]
    }
    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    dp_isna = dp.isna()
    result = dp_isna.to_df()
    
    # same set of columns as the original table, but each entry is a boolean indicating whether the original value was NULL
    expected_columns = ["name", "age", "salary"]
    for col in expected_columns:
        assert col in result.columns, f"Column {col} should be present in the result."
    
    # Check some of the known values.
    assert result.loc[0, "name"] == False, "Expected False for non-null value in 'name'"
    assert result.loc[1, "name"] == True, "Expected True for null value in 'name'"
    assert result.loc[3, "age"] == True, "Expected True for null value in 'age'"
    assert all(result["salary"] == [False, False, False, False]), "All salary entries should be False"

def test_isna_partial():
    data = {
        "name": ["Alice", None, "Charlie", "Diana"],
        "age": [25, 35, 45, None],
        "salary": [50000, 60000, 70000, 80000]
    }
    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    # test a column 
    dp_partial = dp.isna(columns=["age"])
    result_partial = dp_partial.to_df()
    # The result should only include the 'age' column as a boolean field.
    assert result_partial.columns.tolist() == ["age"], "Partial isna should only return the specified column."
    assert result_partial.loc[3, "age"] == True, "Expected True for null value in 'age'"

def test_isna_agg():
    data = {
        "name": ["Alice", None, None, "Diana"],
        "age": [25, 35, 45, None],
        "salary": [50000, 60000, 70000, 80000]
    }

    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    # First, get a DataFrame of booleans via isna()
    dp_isna = dp.isna()
    print(dp_isna.to_sql())
    # Then, aggregate with sum() to count missing values.
    result = dp_isna.sum().to_df()
    assert result["name_sum"][0] == 2, "There should be two missing values in name"
    assert result["age_sum"][0] == 1, "There should be one missing value in age"
    assert result["salary_sum"][0] == 0, "There should be no missing values in salary"

def test_dropna_rows():
    data = {
        "name": ["Alice", None, None, "Diana"],
        "age": [25, 35, 45, None],
        "salary": [50000, 60000, 70000, 80000]
    }

    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    dp_dropped = dp.dropna(axis=0)
    result = dp_dropped.to_df()
    # Every row in the result should have no NULLs.
    assert result.isna().sum().sum() == 0, "There should be no NULL values after dropna(axis=0)"

def test_dropna_columns():
    data = {
        "name": ["Alice", None, None, "Diana"],
        "age": [25, 35, 45, None],
        "salary": [50000, 60000, 70000, 80000]
    }

    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    dp_dropped = dp.dropna(axis=1)
    result = dp_dropped.to_df()
    # In our sample data, both 'name' and 'age' have at least one NULL, so only 'salary' has no NULLs and should be retained.
    expected_columns = ["salary"]
    assert result.columns.tolist() == expected_columns, f"Expected columns {expected_columns}, got {result.columns.tolist()}"

def test_fillna_scalar():
    data = {
        "digits": [16, None, 42, 50],
        "age": [25, None, 45, 55],
        "salary": [50000, 60000, None, 80000]
    }
    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    # Fill missing values with 0
    dp_filled = dp.fillna(0)
    print(dp_filled.to_sql())
    result = dp_filled.to_df()
    
    # Expected outcome: any NULL replaced by 0
    expected = pd.DataFrame({
        "digits": [16.0, 0.0, 42.0, 50.0],
        "age": [25.0, 0.0, 45.0, 55.0],
        "salary": [50000.0, 60000.0, 0.0, 80000.0]
    })
    pd.testing.assert_frame_equal(result, expected)

def test_fillna_dict():
    data = {
        "name": ["Alice", None, "Charlie", "Diana"],
        "age": [25, None, 45, 55],
        "salary": [50000, 60000, None, 80000]
    }
    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    # Fill missing values with different defaults for each column.
    dp_filled = dp.fillna({"name": "Unknown", "age": 99, "salary": 50.0})
    result = dp_filled.to_df()
    
    # Expected outcome: missing in 'name' replaced with "Unknown", in 'age' replaced with 99,
    # and in 'salary' remains NULL.
    expected = pd.DataFrame({
        "name": ["Alice", "Unknown", "Charlie", "Diana"],
        "age": [25.0, 99.0, 45.0, 55.0],
        "salary": [50000.0, 60000.0, 50.0, 80000.0]
    })
    pd.testing.assert_frame_equal(result, expected)

def test_fillna_mean():
    # Create a DataFrame with missing values in numeric columns.
    data = {
        "A": [1.0, 2.0, None, 4.0],
        "B": [10.0, None, 30.0, 40.0]
    }
    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("numeric_table", df)
    dp = Quack("numeric_table", conn=conn)

    # Apply fillna with "mean" to replace NULLs with the average
    dp_filled = dp.fillna("mean")
    result = dp_filled.to_df()

    # Compute expected means
    expected_mean_A = df["A"].mean()  # (1.0 + 2.0 + 4.0) / 3 = 2.33333...
    expected_mean_B = df["B"].mean()  # (10.0 + 30.0 + 40.0) / 3 = 26.66667...

    # Check that the missing value in A (row index 2) is replaced with the mean
    np.testing.assert_allclose(result.loc[2, "A"], expected_mean_A, rtol=1e-5)
    # Check that the missing value in B (row index 1) is replaced with the mean
    np.testing.assert_allclose(result.loc[1, "B"], expected_mean_B, rtol=1e-5)
    
    # Also verify that non-missing values remain unchanged
    assert result.loc[0, "A"] == 1.0
    assert result.loc[3, "A"] == 4.0

def test_fillna_median():
    data = {
        "A": [1.0, None, 3.0, 100.0],  # Median is 3.0
        "B": [10.0, None, 30.0, 40.0]   # Median is 30.0
    }
    conn = duckdb.connect()
    df = pd.DataFrame(data)
    conn.register("median_table", df)
    dp = Quack("median_table", conn=conn)
    
    # Apply fillna with "median" so that NULLs are replaced by the median value.
    dp_filled = dp.fillna("median")
    result = dp_filled.to_df()

    # Compute expected medians
    expected_median_A = np.median([1.0, 3.0, 100.0])
    expected_median_B = np.median([10.0, 30.0, 40.0])
    
    # Check that the missing values are replaced correctly.
    # Here, the missing value in A is at index 1 and in B also at index 1.
    np.testing.assert_allclose(result.loc[1, "A"], expected_median_A, rtol=1e-5)
    np.testing.assert_allclose(result.loc[1, "B"], expected_median_B, rtol=1e-5)





