from fastapi.testclient import TestClient

from app.main import create_app


def test_healthcheck():
    client = TestClient(create_app(initialize_database=False))
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
