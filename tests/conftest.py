import pytest

from bosc.database import Database


@pytest.fixture
def db():
    return Database("test_db")


@pytest.fixture(autouse=True)
def cleanup(db):
    db.drop_all_collections()
    db.drop_all_indexes()
    yield
    db.drop_all_collections()
    db.drop_all_indexes()


@pytest.fixture
def conn():
    return Database("test_db").connection
