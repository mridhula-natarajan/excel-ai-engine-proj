import pandas as pd
import numpy as np
from typing import Dict, Any,  Optional
from pandas import DataFrame
from app.core.logger import get_logger

logger = get_logger(__name__)

def to_serializable(df: DataFrame, max_rows: int = 20):
    """Converts dataframe to a JSON-serializable python types."""

    records = df.head(max_rows).where(pd.notnull(df.head(max_rows)), None).to_dict(orient="records")
    for r in records:
        for k, v in list(r.items()):
            if isinstance(v, (pd.Timestamp, np.datetime64)):
                try:
                    r[k] = pd.to_datetime(v).strftime("%Y-%m-%d")
                except Exception:
                    r[k] = str(v)
            elif isinstance(v, (np.integer, np.floating, np.bool_)):
                r[k] = v.item()
    return records

# Dispatcher
def execute_plan(df: pd.DataFrame, plan: Dict[str, Any], other_tables: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
    """Executes predicted operation plan on extracted excel df and returns the result with status and preview."""
    logger.info("Starting plan execution.")
    try:
        op = plan.get("operation")
        if op == "aggregate":
            return _do_aggregate(df, plan["parameters"])
        elif op == "math":
            return _do_math(df, plan["parameters"])
        elif op == "filter":
            return _do_filter(df, plan["parameters"])
        elif op == "pivot":
            return _do_pivot(df, plan["parameters"])
        elif op == "unpivot":
            return _do_unpivot(df, plan["parameters"])
        elif op == "join":
            return _do_join(df, plan["parameters"], other_tables)
        elif op == "date_ops":
            return _do_date_ops(df, plan["parameters"])
        elif op == "text_analysis":
            return _do_text_analysis(df, plan["parameters"])
        elif op == "describe" or op == "sample":
            return {"status":"ok", "result_df": df, "preview": to_serializable(df, max_rows=10), "message":"describe/sample"}
        elif op == "multi_step":
            # execute steps sequentially
            current_df = df.copy()
            results = None
            for step in plan.get("steps", []):
                out = execute_plan(current_df, step, other_tables=other_tables)
                if out["status"] != "ok":
                    return out
                results = out
                if out.get("result_df") is not None:
                    current_df = out["result_df"]
            return {"status":"ok", "result_df": current_df, "preview": to_serializable(current_df, max_rows=20), "message":"multi_step executed"}
        else:
            return {"status":"error", "message": f"Unsupported operation: {op}"}
    except Exception as e:
        return {"status":"error", "message": str(e)}

# --- Operation implementations ---
def _do_aggregate(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Performs aggregation on a df."""

    col = params.get("column")
    group_by = params.get("group_by") or params.get("by")
    method = params.get("method", "sum")
    if isinstance(group_by, str):
        group_by = [group_by]
    res = df.groupby(group_by)[col].agg(method).reset_index()
    return {"status":"ok", "result_df": res, "preview": to_serializable(res)}

def _do_math(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a new result column by performing the asked mathematical expression on respective df."""

    new_col = params.get("new_column")
    expr = params.get("expression")  

    #taking column names as variables; map them to pandas Series
    local_ns = {}
    for c in df.columns:
        # replace spaces with underscores for variable names to avoid breaking
        varname = c.replace(" ", "_")
        local_ns[varname] = df[c]

    safe_expr = expr
    for c in df.columns:
        safe_expr = safe_expr.replace(c, c.replace(" ", "_"))
    df[new_col] = eval(safe_expr, {"np": np, "__builtins__": {}}, local_ns)

    return {"status":"ok", "result_df": df, "preview": to_serializable(df)}

def _do_filter(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Filters the df based on the given condition and returns the matching rows."""

    condition = params.get("condition")  # input like "Sales > 1000 and Status == 'Delivered'"
    try:
        filtered = df.query(condition)
    except Exception:
        filtered = df.loc[df.apply(lambda row: bool(eval(condition, {"np": np, "__builtins__": {}}, row.to_dict())), axis=1)]
    return {"status":"ok", "result_df": filtered, "preview": to_serializable(filtered)}

def _do_pivot(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Creates pivot table from the df using specified index, columns, and aggregation function."""

    index = params.get("index") or []
    columns = params.get("columns") or []
    values = params.get("values")
    aggfunc = params.get("aggfunc", "sum")
    res = df.pivot_table(index=index, columns=columns, values=values, aggfunc=aggfunc).reset_index()
    # flatten columns if MultiIndex
    res.columns = [f"{a}" if not isinstance(a, tuple) else "_".join([str(x) for x in a if x]) for a in res.columns]
    return {"status":"ok", "result_df": res, "preview": to_serializable(res)}

def _do_unpivot(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Unpivot the specified table."""
    id_vars = params.get("id_vars", [])
    value_vars = params.get("value_vars", [])
    res = pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name="variable", value_name="value")
    return {"status":"ok", "result_df": res, "preview": to_serializable(res)}

def _do_join(df: pd.DataFrame, params: Dict[str, Any], other_tables: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
    """Joins the df with another table."""
    left_on = params.get("left_on")
    right_table = params.get("right_table")  # name
    right_on = params.get("right_on")
    how = params.get("how", "inner")
    if other_tables is None or right_table not in other_tables:
        return {"status":"error", "message": f"Right table {right_table} not found for join"}
    right_df = other_tables[right_table]
    res = df.merge(right_df, how=how, left_on=left_on, right_on=right_on)
    return {"status":"ok", "result_df": res, "preview": to_serializable(res)}

def _do_date_ops(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Performs date operations like extracting month or calculating day differences between date columns."""

    col = params.get("column")
    op = params.get("op")  # e.g., "extract_month", "diff_days"
    if op == "extract_month":
        res = df.copy()
        res[col + "_month"] = pd.to_datetime(res[col]).dt.month
        return {"status":"ok", "result_df": res, "preview": to_serializable(res)}
    elif op == "diff_days":
        col2 = params.get("column2")
        res = df.copy()
        res["diff_days"] = (pd.to_datetime(res[col]) - pd.to_datetime(res[col2])).dt.days
        return {"status":"ok", "result_df": res, "preview": to_serializable(res)}
    else:
        return {"status":"error", "message": f"Unsupported date op: {op}"}

def _do_text_analysis(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Performs basic text analysis on a column, such as sentiment detection or text summarization."""
    col = params.get("column")
    new_col = params.get("new_column", col + "_analysis")
    op = params.get("op", "sentiment")  # or "summary"
    if op == "sentiment":
        df[new_col] = df[col].astype(str).apply(lambda s: "positive" if "good" in s.lower() else ("negative" if "poor" in s.lower() else "neutral"))
    else:
        df[new_col] = df[col].astype(str).apply(lambda s: (s[:150] + "...") if len(s) > 150 else s)
    return {"status":"ok", "result_df": df, "preview": to_serializable(df)}
