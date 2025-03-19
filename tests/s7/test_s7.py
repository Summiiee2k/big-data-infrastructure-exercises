from fastapi.testclient import TestClient
from bdi_api.s7 import s7
import pytest

client = TestClient(s7)

@pytest.fixture
def mock_db(monkeypatch):
    # Mock database connection for testing
    def mock_connect(*args, **kwargs):
        class MockCursor:
            def execute(self, query, params=None):
                pass
            def fetchall(self):
                return []
            def fetchone(self):
                return None
            def close(self):
                pass
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        
        class MockConnection:
            def cursor(self):
                return MockCursor()
            def commit(self):
                pass
            def close(self):
                pass
        
        return MockConnection()
    
    monkeypatch.setattr("psycopg2.connect", mock_connect)

def test_prepare_data(mock_db):
    response = client.post("/aircraft/prepare")
    assert response.status_code == 200
    assert response.json() == "OK"

def test_list_aircraft(mock_db):
    response = client.get("/aircraft/?num_results=10&page=0")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_aircraft_position(mock_db):
    response = client.get("/aircraft/ABC123/positions?num_results=10&page=0")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_aircraft_statistics(mock_db):
    response = client.get("/aircraft/ABC123/stats")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)
    assert "max_altitude_baro" in response.json()
    assert "max_ground_speed" in response.json()
    assert "had_emergency" in response.json()