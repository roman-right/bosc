from uuid import UUID

from ossus.queries.comparison import Eq
from ossus.queries.logical import Or


class TestMiniMongo:
    def test_insert(self, db):
        db.test.insert({"name": "John"})
        result = db.test.find_one({"name": "John"})
        assert result["name"] == "John"
        assert UUID(result["_id"], version=4)

    def test_insert_many(self, db):
        db.test.insert_many([{"name": "John"}, {"name": "Jane"}])
        result = db.test.find({})
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "Jane"

    def test_find(self, db):
        db.test.insert_many([{"name": "John"}, {"name": "Jane"}])
        query = Eq("name", "John")
        result = db.test.find(query)
        assert len(result) == 1
        assert result[0]["name"] == "John"

    def test_find_order_by(self, db):
        db.test.insert_many(
            [{"name": "John"}, {"name": "Jane"}, {"name": "Linda"}]
        )
        result = db.test.find(None, order_by="name", order="DESC")
        assert len(result) == 3
        assert result[0]["name"] == "Linda"
        assert result[1]["name"] == "John"
        assert result[2]["name"] == "Jane"

    def test_find_or(self, db):
        db.test.insert_many(
            [{"name": "John"}, {"name": "Jane"}, {"name": "Linda"}]
        )
        query = Or(Eq("name", "John"), Eq("name", "Jane"))
        result = db.test.find(query, order_by="name")
        assert len(result) == 2
        assert result[0]["name"] == "Jane"
        assert result[1]["name"] == "John"

    def test_find_one(self, db):
        db.test.insert_many(
            [{"name": "John"}, {"name": "Jane"}, {"name": "Linda"}]
        )
        query = Eq("name", "John")
        result = db.test.find_one(query)
        assert result["name"] == "John"

    def test_update(self, db):
        db.test.insert_many([{"name": "John"}, {"name": "Jane"}])
        db.test.update({"name": "John"}, {"name": "John Doe"})
        result = db.test.find_one({"name": "John Doe"})
        assert result["name"] == "John Doe"

        result = db.test.find({})
        assert len(result) == 2
        assert result[1]["name"] == "Jane"
        assert result[0]["name"] == "John Doe"

    def test_update_one(self, db):
        db.test.insert_many([{"name": "John"}, {"name": "Jane"}])
        db.test.update_one({"name": "John"}, {"name": "John Doe"})
        result = db.test.find_one({"name": "John Doe"})
        assert result["name"] == "John Doe"

        result = db.test.find({})
        assert len(result) == 2
        assert result[1]["name"] == "Jane"
        assert result[0]["name"] == "John Doe"

    def test_update_one_order_by(self, db):
        db.test.insert_many([{"name": "John"}, {"name": "Jane"}])
        db.test.update_one({}, {"name": "John Doe"}, order_by="name")
        result = db.test.find({}, order_by="name")
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "John Doe"

    def test_update_all(self, db):
        db.test.insert_many([{"name": "John"}, {"name": "Jane"}])
        db.test.update({}, {"name": "John Doe"})
        result = db.test.find({})
        assert len(result) == 2
        assert result[0]["name"] == "John Doe"
        assert result[1]["name"] == "John Doe"
