from sqlite3 import IntegrityError

import pytest

from ossus.collection import OnConflict
from ossus.queries.find.comparison import Eq


class TestInsert:
    def test_insert(self, collection):
        collection.insert({"name": "John", "age": 25})
        result = collection.find()
        assert len(result) == 1
        assert result[0]["name"] == "John"
        assert result[0]["age"] == 25

    def test_insert_many(self, collection):
        collection.insert_many(
            [{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]
        )
        result = collection.find()
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[0]["age"] == 25
        assert result[1]["name"] == "Jane"
        assert result[1]["age"] == 30

    def test_insert_on_conflict_replace(self, collection):
        print(collection.get_indexes())
        collection.insert({"id": 1, "name": "John", "age": 25})
        collection.insert(
            {"id": 1, "name": "John", "age": 30},
            on_conflict=OnConflict.REPLACE,
        )
        result = collection.find(Eq("name", "John"))
        assert len(result) == 1
        assert result[0]["age"] == 30

    def test_insert_on_conflict_ignore(self, collection):
        collection.insert({"id": 1, "name": "John", "age": 25})
        collection.insert(
            {"id": 1, "name": "John", "age": 30}, on_conflict=OnConflict.IGNORE
        )
        result = collection.find(Eq("name", "John"))
        assert len(result) == 1
        assert result[0]["age"] == 25

    def test_insert_on_conflict_raise(self, collection):
        collection.insert({"id": 1, "name": "John", "age": 25})
        with pytest.raises(IntegrityError):
            collection.insert(
                {"id": 1, "name": "John", "age": 30},
                on_conflict=OnConflict.RAISE,
            )
