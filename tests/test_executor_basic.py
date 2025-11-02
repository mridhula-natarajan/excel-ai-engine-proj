import pandas as pd
from app.core.executor import execute_plan

def test_aggregate_sum():
    df = pd.DataFrame({
        "Region": ["East", "West", "East"],
        "Sales": [10, 20, 30]
    })
    plan = {
        "operation": "aggregate",
        "parameters": {
            "group_by": "Region",
            "column": "Sales",
            "method": "sum"
        }
    }
    out = execute_plan(df, plan)
    assert out["status"] == "ok"
    result_df = out["result_df"]
    assert "Sales_sum" in result_df.columns
    assert result_df.loc[result_df["Region"] == "East", "Sales_sum"].iloc[0] == 40

def test_math_add_columns():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]})
    plan = {
        "operation": "math",
        "parameters": {"formula": "A + B", "new_column": "Total"}
    }
    out = execute_plan(df, plan)
    assert out["status"] == "ok"
    assert "Total" in out["result_df"].columns
    assert out["result_df"]["Total"].tolist() == [11, 22, 33]

def test_filter_rows():
    df = pd.DataFrame({"State": ["CA", "NY", "CA"], "Sales": [10, 20, 30]})
    plan = {
        "operation": "filter",
        "parameters": {"column": "State", "operator": "==", "value": "CA"}
    }
    out = execute_plan(df, plan)
    assert out["status"] == "ok"
    assert len(out["result_df"]) == 2
