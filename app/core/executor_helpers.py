import pandas as pd
import numpy as np
import os
import time
import re
from typing import Dict, Any, Tuple
from pandas import DataFrame
from app.core.logger import get_logger
from app.services.gemini_service import generate_text 

logger = get_logger(__name__)
logger.info("LOADED: %s", os.path.abspath(__file__))


def to_serializable(df: DataFrame, max_rows: int = 20):
    """Converts dataframe to JSON-serializable python types."""
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

def _do_aggregate(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Performs aggregation on a df. Handles missing group_by gracefully."""
    col = params.get("column")
    group_by = params.get("group_by") or params.get("by")
    method = params.get("method", "sum")
    sort_by = params.get("sort_by")
    order = params.get("order", "asc")
    limit = params.get("limit")

    logger.info(f"Aggregating: col={col}, group_by={group_by}, method={method}")

    try:
        if not group_by:
            if col in df.columns:
                agg_val = getattr(df[col], method)()
                res = pd.DataFrame([{f"{col}_{method}": agg_val}])
                logger.info(f"No group_by specified, computed {method}({col}) = {agg_val}")
            else:
                # Try counting rows if col missing
                if method == "count":
                    res = pd.DataFrame([{"count": len(df)}])
                    logger.info(f"No group_by/column, returning row count = {len(df)}")
                else:
                    raise KeyError(f"Column not found: {col}")
        else:
            if isinstance(group_by, str):
                group_by = [group_by]
            res = df.groupby(group_by)[col].agg(method).reset_index()
            new_col_name = f"{col}_{method}" if f"{col}_{method}" not in res.columns else col
            res.rename(columns={col: new_col_name}, inplace=True)
            logger.info(f"Grouped by {group_by} and aggregated {col} with {method}")

        # Optional sorting and limiting
        if sort_by and sort_by in res.columns:
            res = res.sort_values(by=sort_by, ascending=(order.lower() == "asc"))
        if limit:
            res = res.head(int(limit))

        # Save to Excel (summary report)
        os.makedirs("results", exist_ok=True)
        timestamp = int(time.time())
        result_filename = f"result_aggregate_{timestamp}.xlsx"
        result_path = os.path.join("results", result_filename)
        res.to_excel(result_path, index=False)
        logger.info(f"Result saved at {result_path}")

        return {
            "status": "ok",
            "result_df": res,
            "preview": to_serializable(res),
            "message": "Executed successfully",
            "result_file": result_filename,
            "file_path": result_path
        }

    except Exception as e:
        logger.exception(" Error in _do_aggregate: %s", e)
        return {"status": "error", "message": str(e)}

def _do_math(df, params):
    """Perform basic mathematical operations on numerical columns."""
    try:
        new_col = params.get("new_column")
        formula = params.get("formula")

        #  If new column already exists after derivation, return success early
        if new_col and new_col in df.columns:
            return {
                "status": "ok",
                "result_df": df,
                "preview": df.head(5).to_dict(orient="records"),
                "message": f"Column '{new_col}' already present, skipping math."
            }

        #  Try to synthesize formula if not provided by llm
        if not formula:
            col1 = params.get("column1") or (params.get("columns") or [None, None])[0]
            col2 = params.get("column2") or (params.get("columns") or [None, None, None])[1]
            method = (params.get("method") or "").lower().strip()

            op_map = {
                "add": "+", "sum": "+", "plus": "+",
                "subtract": "-", "minus": "-",
                "multiply": "*", "mul": "*", "times": "*",
                "divide": "/", "div": "/"
            }

            if col1 and col2 and method in op_map:
                formula = f"{col1} {op_map[method]} {col2}"
            elif col1 and col2 and params.get("operator"):
                formula = f"{col1} {params['operator']} {col2}"

            if "formula" not in params:
                # Allow split form: column + value + operation
                if all(k in params for k in ["column", "value", "operation"]):
                    col, val, op_type = params["column"], params["value"], params["operation"]
                    if op_type.lower() == "multiply":
                        params["formula"] = f"{col} * {val}"
                    elif op_type.lower() == "divide":
                        params["formula"] = f"{col} / {val}"
                    elif op_type.lower() == "add":
                        params["formula"] = f"{col} + {val}"
                    elif op_type.lower() == "subtract":
                        params["formula"] = f"{col} - {val}"
            if "new_column" not in params and "new_column_name" in params:
                params["new_column"] = params["new_column_name"]

        if not formula or not new_col:
            return {"status": "error", "message": "Missing formula or new_column in parameters"}

        #  Evaluate formula safely using pandas eval
        local_ns = {col: df[col] for col in df.columns if col in formula}
        local_ns.update({"np": np, "pd": pd})
        df[new_col] = eval(formula, {"__builtins__": {}}, local_ns)

        return {
            "status": "ok",
            "result_df": df,
            "preview": df.head(5).to_dict(orient="records"),
            "message": f"Added new column '{new_col}' using formula: {formula}"
        }

    except Exception as e:
        return {"status": "error", "message": f"Math operation failed: {str(e)}"}
    
def _do_filter(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Filters dataframe based on a condition like column, operator, and value."""
    try:
        col = params.get("column")
        op = params.get("operator", "==")
        val = params.get("value")

        if col not in df.columns:
            raise KeyError(f"Column not found: {col}")

        logger.info(f"Filtering: {col} {op} {val}")

        if op == "==":
            filtered = df[df[col] == val]
        elif op == "!=":
            filtered = df[df[col] != val]
        elif op == ">":
            filtered = df[df[col] > val]
        elif op == "<":
            filtered = df[df[col] < val]
        elif op == ">=":
            filtered = df[df[col] >= val]
        elif op == "<=":
            filtered = df[df[col] <= val]
        elif op.lower() in ["contains", "like"]:
            filtered = df[df[col].astype(str).str.contains(str(val), case=False, na=False)]
        else:
            raise ValueError(f"Unsupported operator: {op}")

        logger.info(f"Filtered rows: {len(filtered)} / {len(df)}")

        return {
            "status": "ok",
            "result_df": filtered,
            "preview": to_serializable(filtered),
            "message": f"Filtered rows where {col} {op} {val}"
        }

    except Exception as e:
        logger.exception(" Error in _do_filter: %s", e)
        return {"status": "error", "message": str(e)}

def _do_pivot(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Creates pivot table."""
    index = params.get("index") or []
    columns = params.get("columns") or []
    values = params.get("values")
    aggfunc = params.get("aggfunc", "sum")
    logger.info("Pivoting with index=%s, columns=%s, values=%s", index, columns, values)
    res = df.pivot_table(index=index, columns=columns, values=values, aggfunc=aggfunc).reset_index()
    res.columns = [f"{a}" if not isinstance(a, tuple) else "_".join([str(x) for x in a if x]) for a in res.columns]
    os.makedirs("results", exist_ok=True)
    timestamp = int(time.time())
    result_filename = f"result_{timestamp}.xlsx"
    result_path = os.path.join("results", result_filename)

    try:
        res.to_excel(result_path, index=False)
        logger.info(f"Result saved at {result_path}")
    except Exception as e:
        logger.error(f"Failed to save result: {e}")
        result_path = None

    return {
        "status": "ok",
        "result_df": res,
        "preview": to_serializable(res),
        "message": f"Executed successfully",
        "result_file": result_filename if result_path else None
    }

def _do_unpivot(df, params):
    id_vars = params.get("id_vars", [])
    value_vars = params.get("value_vars", [])
    var_name = params.get("var_name", "variable")
    value_name = params.get("value_name", "value")

    melted = df.melt(id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name)

    result_path = f"results/result_{int(time.time())}.xlsx"
    melted.to_excel(result_path, index=False)

    return {"status": "ok", "result_df": melted, "file_path": result_path}

def _resolve_table_name(name: str, other_tables: dict):
    """Resolve table name from other_tables using normalization and partial matching."""
    if not other_tables or not name:
        return None

    def norm(s: str):
        return re.sub(r'[^a-z0-9]', '', s.lower())

    target = norm(name)
    candidates = {k: norm(k) for k in other_tables.keys()}
    for k, n in candidates.items():
        if n == target:
            return k
    for k, n in candidates.items():
        if target in n or n in target:
            return k
    target_tokens = set(re.findall(r'[a-z]+', target))
    best, bestscore = None, 0
    for k, n in candidates.items():
        cand_tokens = set(re.findall(r'[a-z]+', n))
        score = len(target_tokens & cand_tokens)
        if score > bestscore:
            bestscore, best = score, k

    return best

def _do_join(df_left, params, other_tables=None, logger=None):
    """Perform join operation between two DFs."""
    try:
        if other_tables is None or not isinstance(other_tables, dict) or not other_tables:
            return {"status": "error", "message": "No other tables provided for join."}

        # Extract parameters
        right_table_name = params.get("right_table")
        left_on = params.get("left_on")
        right_on = params.get("right_on")
        join_col = params.get("column")
        how = (params.get("how") or "inner").lower().strip()

        #  Auto-infer join columns if only one is given
        if join_col and not (left_on or right_on):
            left_on = right_on = join_col

        #  Resolve right_table name flexibly (if LLM skipped it, choose first other_table)
        resolved_right = None
        if right_table_name:
            resolved_right = _resolve_table_name(right_table_name, other_tables)
        if not resolved_right:
            # fallback: take the first table key as default right table
            resolved_right = list(other_tables.keys())[0]

        df_right = other_tables.get(resolved_right)
        if df_right is None:
            return {"status": "error", "message": f"Right table '{resolved_right}' not found."}

        if not all([left_on, right_on]):
            return {"status": "error", "message": "Missing left_on or right_on in join parameters."}

        if logger:
            logger.info(f"Joining on '{left_on}' with '{resolved_right}' ({how} join)")

        result_df = df_left.merge(df_right, left_on=left_on, right_on=right_on, how=how)

        return {
            "status": "ok",
            "result_df": result_df,
            "preview": result_df.head(5).to_dict(orient="records"),
            "message": f"Join completed successfully with table '{resolved_right}'"
        }

    except Exception as e:
        return {"status": "error", "message": f"Join operation failed: {str(e)}"}
     
def _do_date_ops(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Performs date operations like extracting month or calculating day differences."""
    col = params.get("column")
    op = params.get("op")
    logger.info("Date operation '%s' on column '%s'", op, col)

    if op == "extract_month":
        res = df.copy()
        res[col + "_month"] = pd.to_datetime(res[col]).dt.month
    elif op == "diff_days":
        col2 = params.get("column2")
        res = df.copy()
        res["diff_days"] = (pd.to_datetime(res[col]) - pd.to_datetime(res[col2])).dt.days
    else:
        return {"status": "error", "message": f"Unsupported date op: {op}"}

    # Save result to Excel
    os.makedirs("results", exist_ok=True)
    timestamp = int(time.time())
    result_filename = f"result_{op}_{timestamp}.xlsx"
    result_path = os.path.join("results", result_filename)

    try:
        res.to_excel(result_path, index=False)
        logger.info(f"Result saved at {result_path}")
    except Exception as e:
        logger.error(f"Failed to save result: {e}")
        result_path = None

    return {
        "status": "ok",
        "result_df": res,
        "preview": to_serializable(res),
        "message": f"{op} executed successfully",
        "result_file": result_filename if result_path else None
    }

def _do_text_analysis(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Performs basic text analysis (sentiment or summary)."""
    col = params.get("column")
    new_col = params.get("new_column", col + "_analysis")
    op = params.get("op", "sentiment")

    logger.info("Performing text analysis (%s) on column '%s'", op, col)
    if op == "sentiment":
        df[new_col] = df[col].astype(str).apply(lambda s: "positive" if "good" in s.lower() else ("negative" if "poor" in s.lower() else "neutral"))
    else:
        df[new_col] = df[col].astype(str).apply(lambda s: (s[:150] + "...") if len(s) > 150 else s)
    # Save result to Excel
    os.makedirs("results", exist_ok=True)
    timestamp = int(time.time())
    result_filename = f"result_{op}_{timestamp}.xlsx"
    result_path = os.path.join("results", result_filename)

    try:
        df.to_excel(result_path, index=False)
        logger.info(f"Result saved at {result_path}")
    except Exception as e:
        logger.error(f"Failed to save result: {e}")
        result_path = None

    return {
        "status": "ok",
        "result_df": df,
        "preview": to_serializable(df),
        "message": f"{op} executed successfully",
        "result_file": result_filename if result_path else None
    }

def _extract_column_mentions(plan: dict) -> set:
    cols = set()
    params = plan.get("parameters", {}) or {}
    def collect(v):
        if isinstance(v, str):
            parts = re.split(r"[,\|\(\)\+\-\*/<>:%\"]+", v)
            for p in parts:
                p = p.strip()
                # consider tokens that contain letters and not a number only
                if p and re.search(r"[A-Za-z]", p):
                    cols.add(p)
        elif isinstance(v, list):
            for el in v:
                collect(el)
        elif isinstance(v, dict):
            for el in v.values():
                collect(el)
    collect(params)
    return cols

def _safe_eval_expression_on_sample(expr: str, df: 'pd.DataFrame') -> Tuple[bool, str]:
    """Evaluate the expression on a small sample of the DataFrame to validate it."""
    sample = df.head(10).copy()
    local_ns = {}
    for c in sample.columns:
        var = c.replace(" ", "_")
        local_ns[var] = sample[c]
    safe_globals = {"np": np, "pd": pd, "__builtins__": {}}
    try:
        # Evaluate expression 
        result = eval(expr, safe_globals, local_ns)
        if hasattr(result, "__len__") and len(result) != len(sample):
            return False, f"Result length mismatch: expected {len(sample)}, got {len(result)}"
        if hasattr(result, "dtype") and result.dtype.kind in ("f", "i"):
            if (result.replace([np.inf, -np.inf], np.nan).isna().all()):
                return False, "Result is entirely NaN/inf"
        return True, None
    except Exception as e:
        return False, str(e)

def _normalize_token_name(token: str) -> str:
    """Convert df column names into py variables."""
    return token.strip().replace(" ", "_").replace("-", "_")

def derive_missing_columns_with_llm(plan: dict, df: 'pd.DataFrame', logger) -> Tuple['pd.DataFrame', dict]:
    """For any column referenced by the plan but missing from df,ask the LLM to propose a derivation using only existing columns."""
    report = {}
    
    if plan.get("operation") == "math" and plan["parameters"].get("formula"):
        logger.info("Skipping LLM derivation (formula provided manually).")
        return df, {}
    # 1) detect mentioned columns from plan
    mentioned = _extract_column_mentions(plan)
    # Filter out obvious non-column words like SQL keywords (very conservative)
    sql_stop = {"select","where","group","by","order","limit","sum","avg","count","and","or","not","desc","asc"}
    mentioned = {m for m in mentioned if m.lower() not in sql_stop}
    # 2) which are missing
    missing = [m for m in mentioned if m not in df.columns]
    if not missing:
        return df, report

    logger.info("Derivation needed for columns: %s", missing)

    # available columns tokens to propose usage
    avail_cols = list(df.columns)
    avail_tokens = { _normalize_token_name(c): c for c in avail_cols }

    sample_cols_display = ", ".join(avail_cols[:30])
    for col in missing:
        skip_keys = {"method", "how", "left_table", "right_table", "operation", "new_column"}
        if col in skip_keys:
            continue
        try:
            prompt = f"""
        You are given a pandas DataFrame with these columns:
        {sample_cols_display}

        The user requested an operation that refers to a missing column named "{col}".
        Propose a single short Python expression (no surrounding code, no markdown fences)
        that computes "{col}" using only the existing columns above. Use pandas Series variable names
        that are the column names with spaces replaced by underscores.

        If the missing column cannot be derived from the available columns, return the single token:
        CANNOT_DERIVE

        Examples of valid outputs:
        Quantity * UnitPrice * (1 - Discount)
        UnitPrice * Quantity
        Profit - Cost

        Return only the expression or CANNOT_DERIVE.
        """     
            logger.info("LLM to derive '%s' from %d columns", col, len(avail_cols))
            raw = generate_text(prompt)
            expr = None
            try:
                text = str(raw).strip()
            except Exception:
                text = str(raw)

            for line in text.splitlines():
                l = line.strip()
                if not l:
                    continue
                # cleaning
                if (l.startswith('"') and l.endswith('"')) or (l.startswith("'") and l.endswith("'")):
                    l = l[1:-1]
                expr = l
                break

            if not expr:
                report[col] = {"status": "failed", "detail": "empty LLM response"}
                logger.warning(" Empty LLM response when deriving %s", col)
                continue

            if expr == "CANNOT_DERIVE" or "cannot" in expr.lower():
                report[col] = {"status": "skipped", "detail": "LLM indicated cannot derive"}
                logger.info("LLM could not derive '%s'", col)
                continue

            # Replace tokens that match existing df cols
            safe_expr = expr
            for token, original_col in avail_tokens.items():
                safe_expr = re.sub(rf"\b{re.escape(original_col)}\b", _normalize_token_name(original_col), safe_expr)
                safe_expr = re.sub(rf"\b{re.escape(token)}\b", token, safe_expr)

            ok, err = _safe_eval_expression_on_sample(safe_expr, df)
            if not ok:
                report[col] = {"status": "failed", "detail": f"validation failed: {err}", "expr": safe_expr}
                logger.warning("Validation failed for derived expr for '%s': %s", col, err)
                continue

            try:
                local_ns = {}
                for c in df.columns:
                    local_ns[_normalize_token_name(c)] = df[c]
                # Execute on full df
                result_series = eval(safe_expr, {"np": np, "pd": pd, "__builtins__": {}}, local_ns)
                df[col] = result_series
                report[col] = {"status": "derived", "detail": "applied", "expr": safe_expr}
                logger.info(" Derived column '%s' applied using expression: %s", col, safe_expr)
            except Exception as e:
                report[col] = {"status": "failed", "detail": f"apply error: {e}", "expr": safe_expr}
                logger.error("Failed to apply derived expr for '%s': %s", col, e)
        except Exception as e:
            report[col] = {"status": "failed", "detail": str(e)}
            logger.exception("Unexpected error while deriving %s: %s", col, e)

    return df, report
