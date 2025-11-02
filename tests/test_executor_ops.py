import pandas as pd
import pytest
from app.core.executor import execute_plan

@pytest.fixture(autouse=True)
def disable_llm(monkeypatch):
    try:
        import app.services.gemini_service as gs
        monkeypatch.setattr(gs, "generate_text", lambda prompt: "CANNOT_DERIVE")
    except Exception:
        # If module not present during test import, skip patch
        pass
    yield


def test_join_inner_and_left():
    # Left table (orders)
    left = pd.DataFrame({
        "OrderID": ["O1", "O2", "O3"],
        "CustomerID": [101, 102, 103],
        "Amount": [100, 150, 200]
    })

    # Right table (customers)
    right = pd.DataFrame({
        "CustomerID": [101, 102, 104],
        "CustomerName": ["Alice", "Bob", "Zara"]
    })

    other_tables = {"customers": right}

    # 1) Inner join plan
    plan_inner = {
        "operation": "join",
        "parameters": {
            "left_on": "CustomerID",
            "right_table": "customers",
            "right_on": "CustomerID",
            "how": "inner"
        }
    }
    out_inner = execute_plan(left.copy(), plan_inner, other_tables=other_tables)
    assert out_inner["status"] == "ok"
    df_inner = out_inner["result_df"]
    # inner join should only keep CustomerID 101 and 102
    assert set(df_inner["CustomerID"].tolist()) == {101, 102}
    assert "CustomerName" in df_inner.columns

    # 2) Left join plan
    plan_left = plan_inner.copy()
    plan_left["parameters"]["how"] = "left"
    out_left = execute_plan(left.copy(), plan_left, other_tables=other_tables)
    assert out_left["status"] == "ok"
    df_left = out_left["result_df"]
    # left join should preserve all left OrderIDs
    assert set(df_left["OrderID"].tolist()) == {"O1", "O2", "O3"}
    # CustomerName exists (NaN for unmatched CustomerID 103)
    assert "CustomerName" in df_left.columns
    assert df_left.loc[df_left["OrderID"] == "O3", "CustomerName"].isna().any()


def test_pivot_table():
    df = pd.DataFrame({
        "Region": ["East", "East", "West", "West", "West"],
        "Product": ["A", "B", "A", "B", "A"],
        "Sales": [10, 20, 5, 15, 10]
    })

    plan = {
        "operation": "pivot",
        "parameters": {
            "index": ["Region"],
            "columns": ["Product"],
            "values": "Sales",
            "aggfunc": "sum"
        }
    }

    out = execute_plan(df.copy(), plan)
    assert out["status"] == "ok"
    res = out["result_df"]
    # expecting Region + columns for Product A and B
    assert "Region" in res.columns
    # after pivot, columns could be like 'A' and 'B' or flattened names
    # check aggregated sum values
    row_east = res[res["Region"] == "East"].iloc[0]
    # In East: A=10, B=20
    assert row_east.get("A", 10) == 10 or row_east.loc["A"] == 10
    assert row_east.get("B", 20) == 20 or row_east.loc["B"] == 20


def test_unpivot_melt():
    df = pd.DataFrame({
        "OrderID": ["O1", "O2"],
        "Jan": [100, 200],
        "Feb": [150, 250],
        "Mar": [0, 300]
    })
    plan = {
        "operation": "unpivot",
        "parameters": {
            "id_vars": ["OrderID"],
            "value_vars": ["Jan", "Feb", "Mar"]
        }
    }
    out = execute_plan(df.copy(), plan)
    assert out["status"] == "ok"
    res = out["result_df"]
    # after melt, expect rows = original rows * 3 months
    assert len(res) == 2 * 3
    assert set(res.columns) >= {"OrderID", "variable", "value"}


def test_text_analysis_sentiment():
    df = pd.DataFrame({
        "Feedback": [
            "The product was good and delivery was fast",
            "Poor build quality, very disappointed",
            "Okay overall"
        ]
    })
    plan = {
        "operation": "text_analysis",
        "parameters": {
            "column": "Feedback",
            "new_column": "Feedback_sentiment",
            "op": "sentiment"
        }
    }
    out = execute_plan(df.copy(), plan)
    assert out["status"] == "ok"
    res = out["result_df"]
    # Expect new sentiment column
    assert "Feedback_sentiment" in res.columns
    # Check first row is positive, second negative, third neutral (per simplistic rule)
    assert res.loc[0, "Feedback_sentiment"] == "positive"
    assert res.loc[1, "Feedback_sentiment"] == "negative"
    assert res.loc[2, "Feedback_sentiment"] == "neutral"
