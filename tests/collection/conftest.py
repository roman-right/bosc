from datetime import datetime

import pytest


@pytest.fixture
def collection(db):
    return db.test_collection


@pytest.fixture
def documents(collection):
    inserted_docs = []
    docs_to_insert = [
        {
            "name": "John",
            "age": 25,
            "address": {"city": "New York", "state": "NY"},
            "created_at": datetime.strptime(
                "2021-01-01", "%Y-%m-%d"
            ).timestamp(),
        },
        {
            "name": "Jane",
            "age": 22,
            "address": {"city": "Los Angeles", "state": "CA"},
            "created_at": datetime.strptime(
                "2021-01-02", "%Y-%m-%d"
            ).timestamp(),
        },
        {
            "name": "Joe",
            "age": 30,
            "address": {"city": "Chicago", "state": "IL"},
            "created_at": datetime.strptime(
                "2021-01-03", "%Y-%m-%d"
            ).timestamp(),
        },
        {
            "name": "John",
            "age": 40,
            "address": {"city": "New York", "state": "NY"},
            "created_at": datetime.strptime(
                "2021-01-04", "%Y-%m-%d"
            ).timestamp(),
        },
        {
            "name": "Linda",
            "age": 27,
            "address": {"city": "Los Angeles", "state": "CA"},
            "created_at": datetime.strptime(
                "2021-01-05", "%Y-%m-%d"
            ).timestamp(),
        },
    ]
    for doc in docs_to_insert:
        res = collection.insert(doc)
        inserted_docs.append(res)

    return inserted_docs
