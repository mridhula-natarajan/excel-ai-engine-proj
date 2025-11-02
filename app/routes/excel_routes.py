from fastapi import APIRouter, HTTPException,UploadFile, File
from pydantic import BaseModel
import pandas as pd
import os
from app.services.gemini_service import generate_text
from app.core.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


class QueryRequest(BaseModel):
    filename: str
    query: str

@router.post("/upload")
async def upload_excel(file: UploadFile = File(...)):
    """Handles Excel file uploads, saves them to disk, and returns the filename with column names."""
    logger.info(f"Received Excel upload: {file.filename}")
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    df = pd.read_excel(file_path)
    return {"filename": file.filename, "columns": df.columns.tolist()}
