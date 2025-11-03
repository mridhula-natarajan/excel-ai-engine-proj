import os
import pandas as pd
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_join_between_two_excels(tmp_path):
    # Prepare small sample Excel files
    df1 = pd.DataFrame({
        "EmpID": [1, 2, 3],
        "Name": ["Alice", "Bob", "Charlie"],
        "DeptID": [101, 102, 103],
    })
    df2 = pd.DataFrame({
        "DeptID": [101, 102, 103],
        "Department": ["HR", "Finance", "R&D"],
    })

    os.makedirs("uploads", exist_ok=True)
    file1 = os.path.join("uploads", "emp.xlsx")
    file2 = os.path.join("uploads", "dept.xlsx")
    df1.to_excel(file1, index=False)
    df2.to_excel(file2, index=False)

    with open(file1, "rb") as f1, open(file2, "rb") as f2:
        response = client.post(
            "/api/v1/analyze_and_query",
            files=[
                ("file", ("emp.xlsx", f1, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
                ("other_file", ("dept.xlsx", f2, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
            ],
            data={"query": "Join emp with dept on DeptID"},
        )

    print("Response JSON:", response.json())
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "ok"
    assert "file_path" in result or result.get("excel_saved") is True
