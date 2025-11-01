from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
import os
from app.core.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)

# path where input files are saved
UPLOAD_DIR = "uploads"

@router.get("/analyze_excel")
async def analyze_excel(filename: str = Query(..., description="Name of the uploaded Excel file")):
    """Analyzes uploaded Excel file to infer column types, compute summary statistics, and flag unstructured text columns."""
    logger.info(f"Analyzing Excel file: {filename}")
    try:
        file_path = os.path.join(UPLOAD_DIR, filename)
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return JSONResponse(content={"error": "File not found."}, status_code=404)

        df = pd.read_excel(file_path)

        # Step 1: Column type inference
        inferred_types = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            if pd.api.types.is_numeric_dtype(df[col]):
                inferred_types[col] = "numeric"
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                inferred_types[col] = "date"
            elif df[col].nunique() < len(df) * 0.05:
                inferred_types[col] = "categorical"
            else:
                inferred_types[col] = "text"

        # Step 2: Summary statistics
        summary = {}
        for col in df.columns:
            data = df[col]
            if inferred_types[col] == "numeric":
                summary[col] = {
                    "mean": float(data.mean()) if not np.isnan(data.mean()) else None,
                    "min": float(data.min()) if not np.isnan(data.min()) else None,
                    "max": float(data.max()) if not np.isnan(data.max()) else None,
                    "missing": int(data.isna().sum())
                }
            elif inferred_types[col] == "categorical":
                summary[col] = {
                    "unique_values": int(data.nunique()),
                    "top_value": str(data.mode().iloc[0]) if not data.mode().empty else None,
                    "missing": int(data.isna().sum())
                }
            elif inferred_types[col] == "date":
                summary[col] = {
                    "min_date": str(data.min().date()) if not data.isna().all() else None,
                    "max_date": str(data.max().date()) if not data.isna().all() else None,
                    "missing": int(data.isna().sum())
                }
            else:  # text/unstructured
                avg_len = data.dropna().apply(lambda x: len(str(x))).mean() if not data.dropna().empty else None
                summary[col] = {
                    "avg_text_length": round(avg_len, 2) if avg_len else None,
                    "missing": int(data.isna().sum())
                }

        # Step 3: Handle unstructured / free text columns (flag)
        unstructured_cols = [col for col, t in inferred_types.items() if t == "text"]

        result = {
            "inferred_column_types": inferred_types,
            "summary": summary,
            "unstructured_columns": unstructured_cols
        }
        logger.info("Analysis completed successfully.")
        return JSONResponse(content=result)

    except Exception as e:
        logger.error(f"Error analyzing Excel file: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)
