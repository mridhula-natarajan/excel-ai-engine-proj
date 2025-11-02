import pandas as pd
import pytest
import json
from app.core.executor import execute_plan
from app.core.llm_interpreter import call_llm_for_plan

@pytest.mark.integration
def test_llm_plan_generation_and_execution_aggregate():
    """ask Gemini to generate a plan for a natural-language query,execute it, and confirm the output looks correct."""

    df = pd.DataFrame({
        "Region": ["East", "West", "East", "North"],
        "Sales": [100, 200, 150, 250]
    })

    user_query = "Find total sales by region in descending order"

    plan_str = call_llm_for_plan(user_query, sample_columns=list(df.columns))
    assert plan_str, "LLM returned empty plan"

    plan = json.loads(plan_str) if isinstance(plan_str, str) else plan_str
    assert "operation" in plan, "Plan missing operation"
    print("Gemini returned plan:", plan)

    # Execute that plan
    result = execute_plan(df.copy(), plan)
    assert result["status"] == "ok", f"Execution failed: {result}"
    res_df = result["result_df"]

    print("\n Resulting DataFrame:\n", res_df)
    assert not res_df.empty
    assert "Region" in res_df.columns

@pytest.mark.integration
def test_llm_plan_math_and_verify():
    """Real LLM test: ask Gemini to generate a simple math operation plan."""
    df = pd.DataFrame({
        "Quantity": [2, 3, 4],
        "Price": [100, 200, 150]
    })
    user_query = "Add a new column Total as Quantity multiplied by Price"

    plan_str = call_llm_for_plan(user_query, sample_columns=list(df.columns))
    import json
    plan = json.loads(plan_str) if isinstance(plan_str, str) else plan_str
    print("\nGemini plan:", plan)

    result = execute_plan(df.copy(), plan)
    assert result["status"] == "ok", f"Execution failed: {result}"
    res_df = result["result_df"]

    print("\n Output DF:\n", res_df)
    assert "Total" in res_df.columns
    assert all(res_df["Total"] == df["Quantity"] * df["Price"])

@pytest.mark.integration
def test_llm_plan_join_example():
    """Test Gemini’s ability to plan a join operation across two sheets."""
    orders = pd.DataFrame({
        "OrderID": [1, 2, 3],
        "CustomerID": [10, 11, 12],
        "Amount": [500, 1000, 750],
    })
    customers = pd.DataFrame({
        "CustomerID": [10, 11, 13],
        "Name": ["Alice", "Bob", "Eve"]
    })

    query = "Join orders with customer names"

    plan_str = call_llm_for_plan(query, sample_columns=list(orders.columns))
    import json
    plan = json.loads(plan_str) if isinstance(plan_str, str) else plan_str
    print("\n Gemini plan for join:", plan)

    result = execute_plan(orders, plan, other_tables={"customers": customers})
    assert result["status"] == "ok"
    df_join = result["result_df"]
    print("\n Joined DF:\n", df_join)
    assert "Name" in df_join.columns