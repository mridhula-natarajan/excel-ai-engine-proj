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

@router.post("/query")
async def query_excel(request: QueryRequest):
    """Processes a user query on an uploaded Excel file using the LLM to generate pandas code and explanations."""

    try:
        file_path = os.path.join(UPLOAD_DIR, request.filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        df = pd.read_excel(file_path)
        sample = df.head(10).to_dict(orient="records")

        prompt = f"""
        You are an AI assistant for Excel data analysis.
        The dataset sample is below (first 10 rows):
        {sample}

        The user query is:
        "{request.query}"

        Based on the dataset, describe what operation should be done
        (for example: aggregation, filter, join, pivot, math op, etc.)
        and return Python pandas code that can perform it.
        Then, explain what the result represents.
        """

        response =  generate_text(prompt)

        if isinstance(response, str):
            llm_output = response
        elif hasattr(response, "text"):
            llm_output = response.text
        elif getattr(response, "candidates", None):
            llm_output = response.candidates[0].content.parts[0].text
        else:
            llm_output = str(response)

        return {"query": request.query, "llm_output": llm_output}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
