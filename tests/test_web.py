import pytest
from fastapi.testclient import TestClient
from app.web import web_app


def test_health_check_endpoints():
    client = TestClient(web_app)

    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "healthy"

    res_v1 = client.get("/api/v1/health")
    assert res_v1.status_code == 200
    assert res_v1.json()["status"] == "healthy"


def test_dashboard_endpoint():
    client = TestClient(web_app)
    res = client.get("/")
    assert res.status_code == 200
    assert "PostgreSQL 10.4" in res.text
    assert "BigQuery" in res.text
