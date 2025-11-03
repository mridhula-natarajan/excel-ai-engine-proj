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

def test_unpivot_data(tmp_path):
    df = pd.DataFrame({
        "Month": ["Jan", "Feb", "Mar"],
        "Sales_2024": [100, 120, 140],
        "Sales_2025": [200, 220, 260]
    })
    file = os.path.join("uploads", "unpivot_test.xlsx")
    df.to_excel(file, index=False)

    response = client.post(
        "/api/v1/query",
        json={"filename": "unpivot_test.xlsx", "query": "Unpivot the Sales columns into a single column"}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert any("Sales" in col for col in data["preview"][0].keys())