from bosc.collection import OrderDirection
from bosc.query.find.comparison import Eq, Gt, Gte, In, Lt, Lte, Neq, Nin
from bosc.query.find.logical import And, Or


class TestFind:
    def test_equals(self, collection, documents):
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "John"

    def test_not_equals(self, collection, documents):
        result = collection.find(Neq("name", "John"))
        assert len(result) == 3
        assert result[0]["name"] != "John"
        assert result[1]["name"] != "John"
        assert result[2]["name"] != "John"

    def test_greater_than(self, collection, documents):
        result = collection.find(Gt("age", 25))
        assert len(result) == 3
        assert result[0]["age"] > 25
        assert result[1]["age"] > 25
        assert result[2]["age"] > 25

    def test_greater_than_or_equals(self, collection, documents):
        result = collection.find(Gte("age", 25))
        assert len(result) == 4
        assert result[0]["age"] >= 25
        assert result[1]["age"] >= 25
        assert result[2]["age"] >= 25
        assert result[3]["age"] >= 25

    def test_less_than(self, collection, documents):
        result = collection.find(Lt("age", 30))
        assert len(result) == 3
        assert result[0]["age"] < 30
        assert result[1]["age"] < 30
        assert result[2]["age"] < 30

    def test_less_than_or_equals(self, collection, documents):
        result = collection.find(Lte("age", 30))
        assert len(result) == 4
        assert result[0]["age"] <= 30
        assert result[1]["age"] <= 30
        assert result[2]["age"] <= 30
        assert result[3]["age"] <= 30

    def test_in(self, collection, documents):
        result = collection.find(In("name", ["John", "Jane"]))
        assert len(result) == 3
        assert result[0]["name"] in ["John", "Jane"]
        assert result[1]["name"] in ["John", "Jane"]
        assert result[2]["name"] in ["John", "Jane"]

    def test_not_in(self, collection, documents):
        result = collection.find(Nin("name", ["John", "Jane"]))
        assert len(result) == 2
        assert result[0]["name"] not in ["John", "Jane"]
        assert result[1]["name"] not in ["John", "Jane"]

    def test_nested_fields(self, collection, documents):
        result = collection.find(Eq("address.city", "New York"))
        assert len(result) == 2
        assert result[0]["address"]["city"] == "New York"
        assert result[1]["address"]["city"] == "New York"

    def test_order_by(self, collection, documents):
        result = collection.find(Eq("name", "John"), order_by="age")
        assert len(result) == 2
        assert result[0]["age"] == 25
        assert result[1]["age"] == 40

        result = collection.find(
            Eq("name", "John"),
            order_by="age",
            order_direction=OrderDirection.DESC,
        )
        assert len(result) == 2
        assert result[0]["age"] == 40
        assert result[1]["age"] == 25

    def test_find_all(self, collection, documents):
        result = collection.find()
        assert len(result) == 5
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "Jane"
        assert result[2]["name"] == "Joe"
        assert result[3]["name"] == "John"
        assert result[4]["name"] == "Linda"

    def test_and(self, collection, documents):
        result = collection.find(And(Eq("name", "John"), Eq("age", 25)))
        assert len(result) == 1
        assert result[0]["name"] == "John"
        assert result[0]["age"] == 25

    def test_or(self, collection, documents):
        result = collection.find(
            Or(Eq("name", "John"), Eq("age", 22)),
            order_by="age",
            order_direction=OrderDirection.DESC,
        )
        assert len(result) == 3
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "John"
        assert result[2]["name"] == "Jane"


class TestFindOne:
    def test_equals(self, collection, documents):
        result = collection.find_one(Eq("name", "John"))
        assert result["name"] == "John"

    def test_not_equals(self, collection, documents):
        result = collection.find_one(Neq("name", "John"))
        assert result["name"] != "John"

    def test_greater_than(self, collection, documents):
        result = collection.find_one(Gt("age", 25))
        assert result["age"] > 25

    def test_greater_than_or_equals(self, collection, documents):
        result = collection.find_one(Gte("age", 25))
        assert result["age"] >= 25

    def test_less_than(self, collection, documents):
        result = collection.find_one(Lt("age", 30))
        assert result["age"] < 30

    def test_less_than_or_equals(self, collection, documents):
        result = collection.find_one(Lte("age", 30))
        assert result["age"] <= 30

    def test_in(self, collection, documents):
        result = collection.find_one(In("name", ["John", "Jane"]))
        assert result["name"] in ["John", "Jane"]

    def test_not_in(self, collection, documents):
        result = collection.find_one(Nin("name", ["John", "Jane"]))
        assert result["name"] not in ["John", "Jane"]

    def test_nested_fields(self, collection, documents):
        result = collection.find_one(Eq("address.city", "New York"))
        assert result["address"]["city"] == "New York"

    def test_order_by(self, collection, documents):
        result = collection.find_one(Eq("name", "John"), order_by="age")
        assert result["age"] == 25

        result = collection.find_one(
            Eq("name", "John"),
            order_by="age",
            order_direction=OrderDirection.DESC,
        )
        assert result["age"] == 40

    def test_find_all(self, collection, documents):
        result = collection.find_one()
        assert result["name"] == "John"

    def test_offset(self, collection, documents):
        result = collection.find(Eq("name", "John"), offset=1)
        assert len(result) == 1
        assert result[0]["age"] == 40

    def test_limit(self, collection, documents):
        result = collection.find(Eq("name", "John"), limit=1)
        assert len(result) == 1
        assert result[0]["age"] == 25
        result = collection.find(Eq("name", "John"), limit=1, offset=1)
        assert len(result) == 1
        assert result[0]["age"] == 40
