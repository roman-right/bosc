from sqlite3 import IntegrityError

import pytest

from bosc import Document
from bosc.collection import OnConflict


class Sample(Document):
    name: str
    age: int

    bosc_database_path = "test_db"


class TestDocumentInsert:
    def test_insert(self):
        sample = Sample(name="John", age=25)
        sample.insert()
        assert sample.id is not None
        result = Sample.get(sample.id)
        assert result.name == "John"
        assert result.age == 25

    def test_insert_on_conflict_replace(self):
        sample = Sample(name="John", age=25)
        sample.insert()
        sample.age = 30
        sample.insert(on_conflict=OnConflict.REPLACE)
        result = Sample.get(sample.id)
        assert result.name == "John"
        assert result.age == 30

    def test_insert_on_conflict_ignore(self):
        sample = Sample(name="John", age=25)
        sample.insert()
        sample.age = 30
        sample.insert(on_conflict=OnConflict.IGNORE)
        result = Sample.get(sample.id)
        assert result.name == "John"
        assert result.age == 25

    def test_insert_on_conflict_raise(self):
        sample = Sample(name="John", age=25)
        sample.insert()
        sample.age = 30
        with pytest.raises(IntegrityError):
            sample.insert(on_conflict=OnConflict.RAISE)


class TestInsertMany:
    def test_insert_many(self):
        Sample.insert_many(
            [Sample(name="John", age=25), Sample(name="Jane", age=30)]
        )
        result = Sample.find()
        assert len(result) == 2
        assert result[0].name == "John"
        assert result[0].age == 25
        assert result[1].name == "Jane"
        assert result[1].age == 30

    def test_insert_many_on_conflict_replace(self):
        sample_1 = Sample(name="John", age=25)
        sample_2 = Sample(name="Jane", age=30)
        Sample.insert_many([sample_1, sample_2])
        sample_1.age = 35
        sample_2.age = 40
        Sample.insert_many(
            [sample_1, sample_2],
            on_conflict=OnConflict.REPLACE,
        )
        result = Sample.find()
        assert len(result) == 2
        assert result[0].age == 35
        assert result[1].age == 40

    def test_insert_many_on_conflict_ignore(self):
        sample_1 = Sample(name="John", age=25)
        sample_2 = Sample(name="Jane", age=30)
        Sample.insert_many([sample_1, sample_2])
        sample_1.age = 35
        sample_2.age = 40
        Sample.insert_many(
            [sample_1, sample_2],
            on_conflict=OnConflict.IGNORE,
        )
        result = Sample.find()
        assert len(result) == 2
        assert result[0].age == 25
        assert result[1].age == 30

    def test_insert_many_on_conflict_raise(self):
        sample_1 = Sample(name="John", age=25)
        sample_2 = Sample(name="Jane", age=30)
        Sample.insert_many([sample_1, sample_2])
        sample_1.age = 35
        sample_2.age = 40
        with pytest.raises(IntegrityError):
            Sample.insert_many(
                [sample_1, sample_2],
                on_conflict=OnConflict.RAISE,
            )
