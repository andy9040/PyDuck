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

def test_drop_columns():
    dp = setup_duck()
    
    # Drop a single column
    result_single = dp.drop(columns="age").to_df()
    assert "age" not in result_single.columns
    assert "name" in result_single.columns
    assert "salary" in result_single.columns
    assert result_single.shape[1] == 2  # should have 2 columns left

    # Drop multiple columns
    result_multiple = dp.drop(columns=["name", "salary"]).to_df()
    assert "name" not in result_multiple.columns
    assert "salary" not in result_multiple.columns
    assert "age" in result_multiple.columns
    assert result_multiple.shape[1] == 1  # only one column left


# Integration test 
def test_full_pipeline():
    data = {
        "name": ["Alice", "Bob", "Charlie", None],
        "age": [25, 35, None, 55],
        "salary": [50000, 60000, 70000, None],
        "department": ["HR", "Engineering", "HR", "Marketing"]
    }

    df = pd.DataFrame(data)
    conn = duckdb.connect()
    conn.register("people", df)
    dp = Quack("people", conn=conn)

    # Full pipeline: fillna, assign, filter, get_dummies, drop, groupby + agg, rename
    result = (
    dp
    .fillna({"age": 99, "salary": 0})
    .filter("age < 100")
    .get_dummies("department", ["HR", "Engineering"], inplace=False)
    .drop(columns="department")
    .assign(income_bracket="salary > 55000")  # moved later!
    .groupby("income_bracket")
    .agg({"salary": "mean", "age": "max"})
    .rename({"salary_mean": "avg_salary", "age_max": "oldest"})
    .to_df()
    )

    # Expected columns and row count
    assert "avg_salary" in result.columns
    assert "oldest" in result.columns
    assert "income_bracket" in result.columns
    assert result.shape[0] == 2  # True and False brackets

# def test_drop_duplicates():
#     df = pd.DataFrame({
#         "brand": ["Yum Yum", "Yum Yum", "Indomie", "Indomie", "Indomie"],
#         "style": ["cup", "cup", "cup", "pack", "pack"],
#         "rating": [4, 4, 3.5, 15, 5]
#     })

#     conn = duckdb.connect()
#     conn.register("ramen", df)
#     dp = Quack("ramen", conn=conn)

#     # Drop full row duplicates
#     result = dp.drop_duplicates().to_df()
#     assert result.shape[0] == 4

#     # Drop based on brand only
#     result = dp.drop_duplicates(subset="brand").to_df()
#     assert set(result["brand"]) == {"Yum Yum", "Indomie"}


def test_drop_duplicates():
    df = pd.DataFrame({
        "brand": ["Yum Yum", "Yum Yum", "Indomie", "Indomie", "Indomie"],
        "style": ["cup", "cup", "cup", "pack", "pack"],
        "rating": [4, 4, 3.5, 15, 5]
    })

    conn = duckdb.connect()
    conn.register("ramen", df)
    dp = Quack("ramen", conn=conn)

    # Case 1: Drop duplicate rows across all columns
    result_all = dp.drop_duplicates().to_df()
    expected_all = pd.DataFrame({
        "brand": ["Yum Yum", "Indomie", "Indomie", "Indomie"],
        "style": ["cup", "cup", "pack", "pack"],
        "rating": [4, 3.5, 15, 5]
    })
    pd.testing.assert_frame_equal(result_all.sort_values(by=["brand", "style", "rating"]).reset_index(drop=True),
                                   expected_all.sort_values(by=["brand", "style", "rating"]).reset_index(drop=True))

    # Case 2: Drop duplicates based only on 'brand'
    result_brand = dp.drop_duplicates(subset="brand").to_df()
    assert set(result_brand["brand"]) == {"Yum Yum", "Indomie"}
    assert result_brand.drop_duplicates(subset="brand").shape[0] == 2

    # Case 3: Drop duplicates based on ['brand', 'style']
    result_brand_style = dp.drop_duplicates(subset=["brand", "style"]).to_df()
    expected_combos = {
        ("Yum Yum", "cup"),
        ("Indomie", "cup"),
        ("Indomie", "pack")
    }
    actual_combos = set(zip(result_brand_style["brand"], result_brand_style["style"]))
    assert actual_combos == expected_combos
    assert result_brand_style.shape[0] == 3

    # Case 4: Ensure ROW_NUMBER keeps only the first occurrence
    # e.g., the first Yum Yum cup 4 rating should be preserved
    assert result_brand_style[(result_brand_style["brand"] == "Yum Yum") & (result_brand_style["style"] == "cup")]["rating"].values[0] == 4

    print("All drop_duplicates test cases passed.")

def test_sample():
    df = pd.DataFrame({
        "id": range(10),
        "value": [x * 10 for x in range(10)]
    })
    conn = duckdb.connect()
    conn.register("sample_table", df)
    dp = Quack("sample_table", conn=conn)

    # Sample 3 rows without replacement
    result_n = dp.sample(n=3, random_state=42).to_df()
    assert result_n.shape[0] == 3
    assert set(result_n.columns) == {"id", "value"}

    # Sample 50% with replacement
    result_frac = dp.sample(frac=0.5, replace=True, random_state=1).to_df()
    assert result_frac.shape[0] == 5


# Dropna + Rename + Groupby + Agg
def test_dropna_rename_groupby_agg():
    df = pd.DataFrame({
        "dept": ["Sales", "Sales", "HR", "HR", None],
        "salary": [50000, None, 60000, 55000, 40000],
        "level": ["junior", "senior", "senior", "junior", "junior"]
    })

    conn = duckdb.connect()
    conn.register("employees", df)
    dp = Quack("employees", conn=conn)

    result = (
        dp
        .dropna(axis=0)
        .rename({"dept": "department"})
        .groupby("department")
        .agg({"salary": "mean"})
        .to_df()
    )

    assert result.shape[0] == 2
    assert "salary_mean" in result.columns
    assert set(result["department"]) == {"Sales", "HR"}


# Assign + Drop Duplicates + Filter
def test_assign_drop_duplicates_filter():
    df = pd.DataFrame({
        "user": ["A", "B", "B", "C", "A", "C"],
        "clicks": [5, 10, 10, 7, 5, 7]
    })

    conn = duckdb.connect()
    conn.register("logs", df)
    dp = Quack("logs", conn=conn)

    result = (
        dp
        .assign(double_clicks="clicks * 2")
        .drop_duplicates()
        .filter("double_clicks > 10")
        .to_df()
    )

    assert result.shape[0] == 2
    assert set(result["user"]) == {"B", "C"}
    assert all(result["double_clicks"] > 10)


def test_merge_quack():
    df1 = pd.DataFrame({"lkey": ["foo", "bar", "baz", "foo"], "value": [1, 2, 3, 5]})
    df2 = pd.DataFrame({"rkey": ["foo", "bar", "baz", "foo"], "value": [5, 6, 7, 8]})

    conn = duckdb.connect()
    conn.register("left_table", df1)
    conn.register("right_table", df2)

    left = Quack("left_table", conn=conn)
    right = Quack("right_table", conn=conn)

    result = left.merge(right, left_on="lkey", right_on="rkey", suffixes=("_left", "_right")).to_df()
    print(result)

    assert "lkey" in result.columns
    assert "rkey" in result.columns
    assert "value_left" in result.columns
    assert "value_right" in result.columns
    assert result.shape[0] == 6

def test_merge_join_types():
    df1 = pd.DataFrame({"a": ["foo", "bar"], "b": [1, 2]})
    df2 = pd.DataFrame({"a": ["foo", "baz"], "c": [3, 4]})

    conn = duckdb.connect()
    conn.register("left_df", df1)
    conn.register("right_df", df2)

    q1 = Quack("left_df", conn=conn)
    q2 = Quack("right_df", conn=conn)

    # INNER JOIN
    inner = q1.merge(q2, how="inner", on="a").to_df()
    assert inner.shape == (1, 3)
    assert inner["a"].iloc[0] == "foo"
    assert inner["b"].iloc[0] == 1
    assert inner["c"].iloc[0] == 3
    # print(inner)

    # LEFT JOIN
    left = q1.merge(q2, how="left", on="a").to_df()
    assert left.shape == (2, 3)
    assert left["a"].iloc[1] == "bar"
    assert pd.isna(left["c"].iloc[1])
    # print(left)

    # RIGHT JOIN
    right = q1.merge(q2, how="right", on="a").to_df()
    # print(right)
    assert right.shape == (2, 3)
    assert right["a"].iloc[1] == "baz"
    assert pd.isna(right["b"].iloc[1])


def test_merge_with_assign_and_filter():
    df1 = pd.DataFrame({"user": ["u1", "u2"], "score": [50, 80]})
    df2 = pd.DataFrame({"user": ["u1", "u2"], "bonus": [10, 5]})

    conn = duckdb.connect()
    conn.register("users", df1)
    conn.register("rewards", df2)

    users = Quack("users", conn=conn)
    rewards = Quack("rewards", conn=conn)

    result = (
        users.merge(rewards, on="user")
             .assign(final="score + bonus")
             .filter("final >= 60")
             .to_df()
    )

    # print(result)

    assert result.shape == (2, 4)
    assert set(result["user"]) == {"u1", "u2"}
    assert set(result["final"]) == {60, 85}

def test_merge_with_fillna():
    df1 = pd.DataFrame({"id": [1, 2], "val": [100, 200]})
    df2 = pd.DataFrame({"id": [1], "extra": [999]})

    conn = duckdb.connect()
    conn.register("main", df1)
    conn.register("info", df2)

    main = Quack("main", conn=conn)
    info = Quack("info", conn=conn)

    # result = (
    #     main.merge(info, on="id", how="left")
    #         .fillna({"extra": 0})
    #         .to_df()
    # )

    main = main.merge(info, on="id", how="left").fillna({"extra": 0})
    
    print(main.to_sql())
    result = main.to_df()
    assert result.shape == (2, 3)
    assert result.loc[result["id"] == 2, "extra"].iloc[0] == 0





# def test_join_quack():
#     df1 = pd.DataFrame({"key": ["K0", "K1", "K2", "K3"], "A": ["A0", "A1", "A2", "A3"]})
#     df2 = pd.DataFrame({"key": ["K0", "K1", "K2"], "B": ["B0", "B1", "B2"]})

#     conn = duckdb.connect()
#     conn.register("left_table", df1)
#     conn.register("right_table", df2)

#     q1 = Quack("left_table", conn=conn)
#     q2 = Quack("right_table", conn=conn)

#     joined = q1.join(q2, on="key", lsuffix="_left", rsuffix="_right").to_df()
#     print(joined)

#     assert "key" in joined.columns
#     assert "A_left" in joined.columns or "A" in joined.columns
#     assert "B_right" in joined.columns or "B" in joined.columns
#     assert joined.shape[0] == 4
