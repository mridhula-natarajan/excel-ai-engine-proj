# app/core/file_manager.py
from fastapi import UploadFile
from pathlib import Path
import pandas as pd
from typing import Dict, Any
from app.core.logger import get_logger
logger = get_logger(__name__)

# dataset_cache: filename -> { sheet_name -> dataframe }
dataset_cache: Dict[str, Dict[str, pd.DataFrame]] = {}

async def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    """
    Save FastAPI UploadFile to disk.
    """
    # write chunk-wise
    with destination.open("wb") as buffer:
        while True:
            chunk = await upload_file.read(1024*1024)
            if not chunk:
                break
            buffer.write(chunk)

def _read_all_sheets_to_cache(filepath: str) -> Dict[str, pd.DataFrame]:
    """
    Read all sheets in excel.
    """
    sheets = pd.read_excel(filepath, sheet_name=None, engine="openpyxl")
    return sheets

def load_excel_preview(filepath: str, max_rows: int = 5) -> Dict[str, Any]:
    """
    Reads excel, stores Dataframes in cache, returns preview dict
    """
    logger.info(f"Attempting to load Excel preview: {filepath}")
    filepath = str(filepath)
    sheets = _read_all_sheets_to_cache(filepath)
    # store in cache under the filename (basename)
    filename = Path(filepath).name
    dataset_cache[filename] = sheets

    preview = {}
    for sheet_name, df in sheets.items():
        preview_rows = df.head(max_rows).fillna("").to_dict(orient="records")
        dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
        preview[sheet_name] = {
            "nrows": int(df.shape[0]),
            "ncols": int(df.shape[1]),
            "columns": list(df.columns.astype(str)),
            "dtypes": dtypes,
            "preview_rows": preview_rows
        }
    return preview