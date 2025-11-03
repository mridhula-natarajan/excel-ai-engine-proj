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

def test_pivot_summary(tmp_path):
    df = pd.DataFrame({
        "Department": ["HR", "HR", "IT", "IT", "Finance"],
        "Salary": [50000, 60000, 70000, 80000, 90000]
    })
    file = os.path.join("uploads", "pivot_test.xlsx")
    df.to_excel(file, index=False)

    response = client.post(
        "/api/v1/query",
        json={"filename": "pivot_test.xlsx", "query": "Pivot by Department and show average salary"}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "preview" in data
