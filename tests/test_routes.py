import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health_route():
    """Simple sanity check for server health."""
    res = client.get("/health")
    assert res.status_code == 200
    data = res.json()
    assert data.get("status") == "ok"

def test_query_missing_fields():
    """Verify that missing query fields return 400."""
    res = client.post("/api/v1/query", json={})
    assert res.status_code == 400
    assert "required" in res.text.lower() or "filename" in res.text.lower()
