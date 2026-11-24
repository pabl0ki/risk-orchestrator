import pytest
from backend.database.connection import OrchestratorDB

@pytest.fixture
def db():
    # Передаём :memory: для тестов
    instance = OrchestratorDB(":memory:") 
    yield instance
    instance.close()

def test_schema_creation(db):
    """Проверяет, что таблицы создались."""
    tables = db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = [t['name'] for t in tables]
    assert 'bp_request' in table_names
    assert 'lock' in table_names

def test_deduplication(db):
    """Проверяет, что дубликаты схлопываются."""
    bp_id = db.create_business_process("Test", "credit", [])
    
    res1 = db.create_bp_request(bp_id, "2026-11-24", "manual", "user1", {"a": 1})
    assert res1["status"] == "created"
    
    res2 = db.create_bp_request(bp_id, "2026-11-24", "manual", "user2", {"a": 1})
    assert res2["status"] == "merged"
    assert res2["id"] == res1["id"]