import pandas as pd
from app.core.executor import execute_plan

def test_missing_column_error():
    df = pd.DataFrame({"Sales": [10, 20]})
    plan = {
        "operation": "filter",
        "parameters": {"column": "State", "operator": "==", "value": "CA"}
    }
    out = execute_plan(df, plan)
    assert out["status"] == "error"
    assert "Missing" in out["message"] or "not found" in out["message"]
