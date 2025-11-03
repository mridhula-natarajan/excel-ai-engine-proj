import os
import pytest
import pandas as pd
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)
UPLOADS_DIR = "uploads"
RESULTS_DIR = "results"

@pytest.fixture(scope="module", autouse=True)
def setup_test_dirs():
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

def test_basic_text_analysis(tmp_path):
    df = pd.DataFrame({
        "Feedback": [
            "The product is great and reliable",
            "Not satisfied with customer support",
            "Excellent performance, will buy again",
        ]
    })
    file = os.path.join("uploads", "text_test.xlsx")
    df.to_excel(file, index=False)

    response = client.post(
        "/api/v1/query",
        json={"filename": "text_test.xlsx", "query": "Perform basic text analysis on feedback column"}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "preview" in data
