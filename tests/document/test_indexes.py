from pear import Document
from pear import Index, IndexType


class Sample(Document):
    name: str
    age: int
    pear_database_path = "test_db"
    pear_indexes = [
        Index("name", IndexType.UNIQUE),
        Index("age"),
    ]


class TestIndexes:
    def test_sync_indexes(self):
        indexes = Sample.get_collection().get_indexes()
        Sample.sync_indexes()
        new_indexes = Sample.get_collection().get_indexes()
        assert len(new_indexes) == len(indexes) + 2
