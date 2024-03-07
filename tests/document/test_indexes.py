from bosc import Document, Index, IndexType


class Sample(Document):
    name: str
    age: int
    bosc_database_path = "test_db"
    bosc_indexes = [
        Index("name", IndexType.UNIQUE),
        Index("age"),
    ]


class TestIndexes:
    def test_sync_indexes(self):
        indexes = Sample.get_collection().get_indexes()
        Sample.sync_indexes()
        new_indexes = Sample.get_collection().get_indexes()
        assert len(new_indexes) == len(indexes) + 2
