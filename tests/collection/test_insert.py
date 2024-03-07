from sqlite3 import IntegrityError

import pytest

from bosc.collection import OnConflict
from bosc.query.find.comparison import Eq


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


class TestInsertMany:
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

    def test_insert_many_on_conflict_replace(self, collection):
        collection.insert_many(
            [
                {"id": 1, "name": "John", "age": 25},
                {"id": 2, "name": "Jane", "age": 30},
            ]
        )
        collection.insert_many(
            [
                {"id": 1, "name": "John", "age": 30},
                {"id": 2, "name": "Jane", "age": 35},
            ],
            on_conflict=OnConflict.REPLACE,
        )
        result = collection.find()
        assert len(result) == 2
        assert result[0]["age"] == 30
        assert result[1]["age"] == 35

        collection.insert_many(
            [
                {"id": 3, "name": "John", "age": 25},
                {"id": 3, "name": "Jane", "age": 30},
            ],
            on_conflict=OnConflict.REPLACE,
        )
        result = collection.find()
        assert len(result) == 3
        assert result[0]["age"] == 30
        assert result[1]["age"] == 35
        assert result[2]["age"] == 30

    def test_insert_many_on_conflict_ignore(self, collection):
        collection.insert_many(
            [
                {"id": 1, "name": "John", "age": 25},
                {"id": 2, "name": "Jane", "age": 30},
            ]
        )
        collection.insert_many(
            [
                {"id": 1, "name": "John", "age": 30},
                {"id": 2, "name": "Jane", "age": 35},
            ],
            on_conflict=OnConflict.IGNORE,
        )
        result = collection.find()
        assert len(result) == 2
        assert result[0]["age"] == 25
        assert result[1]["age"] == 30

        collection.insert_many(
            [
                {"id": 3, "name": "John", "age": 25},
                {"id": 3, "name": "Jane", "age": 30},
            ],
            on_conflict=OnConflict.IGNORE,
        )
        result = collection.find()
        assert len(result) == 3
        assert result[0]["age"] == 25
        assert result[1]["age"] == 30
        assert result[2]["age"] == 25

    def test_insert_many_on_conflict_raise(self, collection):
        collection.insert_many(
            [
                {"id": 1, "name": "John", "age": 25},
                {"id": 2, "name": "Jane", "age": 30},
            ]
        )
        with pytest.raises(IntegrityError):
            collection.insert_many(
                [
                    {"id": 1, "name": "John", "age": 30},
                    {"id": 2, "name": "Jane", "age": 35},
                ],
                on_conflict=OnConflict.RAISE,
            )

        with pytest.raises(IntegrityError):
            collection.insert_many(
                [
                    {"id": 3, "name": "John", "age": 25},
                    {"id": 3, "name": "Jane", "age": 30},
                ],
                on_conflict=OnConflict.RAISE,
            )
