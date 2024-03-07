from bosc import Eq, Gt, Gte, Lt, Lte
from bosc.collection import OrderDirection
from tests.document.models import Sample


class TestFind:
    def test_find(self, samples):
        result = Sample.find()
        assert len(result) == 4

    def test_find_eq(self, samples):
        result = Sample.find(Eq("name", "John"))
        assert len(result) == 2
        assert result[0].name == "John"
        assert result[0].age == 25
        assert result[1].name == "John"
        assert result[1].age == 40

        result = Sample.find(Eq("name", "John"), order_by="age")
        assert len(result) == 2
        assert result[0].name == "John"
        assert result[0].age == 25
        assert result[1].name == "John"
        assert result[1].age == 40

        result = Sample.find(
            Eq("name", "John"),
            order_by="age",
            order_direction=OrderDirection.DESC,
        )
        assert len(result) == 2
        assert result[0].name == "John"
        assert result[0].age == 40
        assert result[1].name == "John"
        assert result[1].age == 25

    def test_find_many_queries(self, samples):
        result = Sample.find(Eq("name", "John"), Eq("age", 25))
        assert len(result) == 1
        assert result[0].name == "John"
        assert result[0].age == 25

        result = Sample.find(Sample.name == "John", Sample.age == 25)
        assert len(result) == 1
        assert result[0].name == "John"
        assert result[0].age == 25

    def test_find_one(self, samples):
        result = Sample.find_one(Eq("name", "John"))
        assert result.name == "John"
        assert result.age == 25

        result = Sample.find_one(Eq("name", "John"), order_by="age")
        assert result.name == "John"
        assert result.age == 25

        result = Sample.find_one(
            Eq("name", "John"),
            order_by="age",
            order_direction=OrderDirection.DESC,
        )
        assert result.name == "John"
        assert result.age == 40

    def test_count(self, samples):
        result = Sample.count()
        assert result == 4

        result = Sample.count(Eq("name", "John"))
        assert result == 2

    def test_magic_methods(self, samples):
        query = Sample.name == "John"
        assert query == Eq("name", "John")
        result = Sample.find(query)
        assert len(result) == 2
        assert result[0].name == "John"
        assert result[0].age == 25
        assert result[1].name == "John"
        assert result[1].age == 40

        query = Sample.age > 30
        assert query == Gt("age", 30)
        result = Sample.find(query)
        assert len(result) == 2
        assert result[0].name == "Jack"
        assert result[0].age == 35
        assert result[1].name == "John"
        assert result[1].age == 40

        query = Sample.age >= 30
        assert query == Gte("age", 30)
        result = Sample.find(query)
        assert len(result) == 3
        assert result[0].name == "Jane"
        assert result[0].age == 30
        assert result[1].name == "Jack"
        assert result[1].age == 35
        assert result[2].name == "John"
        assert result[2].age == 40

        query = Sample.age < 30
        assert query == Lt("age", 30)
        result = Sample.find(query)
        assert len(result) == 1
        assert result[0].name == "John"
        assert result[0].age == 25

        query = Sample.age <= 30
        assert query == Lte("age", 30)
        result = Sample.find(query)
        assert len(result) == 2
        assert result[0].name == "John"
        assert result[0].age == 25
        assert result[1].name == "Jane"
        assert result[1].age == 30
