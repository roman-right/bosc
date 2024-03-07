from datetime import datetime

from bosc.query.find.comparison import Eq
from bosc.query.update.values import Inc, Now, RemoveField, Set


class TestUpdate:
    def test_update(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = Set("age", 500)
        collection.update(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert result[0]["age"] == 500
        assert result[1]["age"] == 500

    def test_update_nested(self, collection, documents):
        find_query = Eq("address.city", "New York")
        update_query = Set("address.city", "San Francisco")
        collection.update(find_query, update_query)
        result = collection.find(Eq("address.city", "San Francisco"))
        assert len(result) == 2
        assert result[0]["address"]["city"] == "San Francisco"
        assert result[0]["address"]["state"] == "NY"
        assert result[1]["address"]["city"] == "San Francisco"
        assert result[1]["address"]["state"] == "NY"

    def test_update_now(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = Now("created_at")
        collection.update(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        print(result[0]["created_at"])
        assert len(result) == 2
        assert result[0]["created_at"] is not None
        assert result[1]["created_at"] is not None
        assert datetime.now().timestamp() - result[0]["created_at"] < 5
        assert datetime.now().timestamp() - result[1]["created_at"] < 5

    def test_update_remove_field(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = RemoveField("address")
        collection.update(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert "address" not in result[0]
        assert "address" not in result[1]

    def test_update_inc(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = Inc("age", 5)
        collection.update(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert result[0]["age"] == 30
        assert result[1]["age"] == 45

    def test_update_all(self, collection, documents):
        update_query = Set("age", 500)
        collection.update(None, update_query)
        result = collection.find()
        assert len(result) == 5
        for doc in result:
            assert doc["age"] == 500


class TestUpdateOne:
    def test_update_one(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = Set("age", 500)
        collection.update_one(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert result[0]["age"] == 500
        assert result[1]["age"] != 500
        assert result[1]["age"] == 40

    def test_update_one_now(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = Now("created_at")
        collection.update_one(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert result[0]["created_at"] is not None
        assert result[1]["created_at"] is not None
        assert datetime.now().timestamp() - result[0]["created_at"] < 5
        assert result[1]["created_at"] != result[0]["created_at"]
        assert datetime.now().timestamp() - result[1]["created_at"] > 5

    def test_update_one_inc(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = Inc("age", 5)
        collection.update_one(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert result[0]["age"] == 30
        assert result[1]["age"] == 40

    def test_update_one_remove_field(self, collection, documents):
        find_query = Eq("name", "John")
        update_query = RemoveField("address")
        collection.update_one(find_query, update_query)
        result = collection.find(Eq("name", "John"))
        assert len(result) == 2
        assert "address" not in result[0]
        assert "address" in result[1]

    def test_update_one_nested(self, collection, documents):
        find_query = Eq("address.city", "New York")
        update_query = Set("address.city", "San Francisco")
        collection.update_one(find_query, update_query)
        result = collection.find(Eq("address.city", "San Francisco"))
        assert len(result) == 1
        assert result[0]["address"]["city"] == "San Francisco"
        assert result[0]["address"]["state"] == "NY"
