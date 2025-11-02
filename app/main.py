__version__ = "0.1.0"
__author__ = "Mridhula N"

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from app.routes import analyze_routes
from app.routes.excel_routes import router as excel_router
from app.routes.query_routes import router as query_router
from app.routes.all_routes import router as main_router


app = FastAPI(
    title="Excel AI Engine ",
    description="Upload Excel files and preview them.",
    version="0.1.0"
)
app.include_router(main_router, prefix="/api/v1", tags=["Analyze and Query Excel"])

app.include_router(excel_router, prefix="/api/v1")
app.include_router(analyze_routes.router, prefix="/api/v1")
app.include_router(query_router, prefix="/api/v1")


@app.get("/health")
def health():
    return {"status": "ok"}