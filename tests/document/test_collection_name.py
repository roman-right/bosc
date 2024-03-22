from bosc import Document


class SampleWithoutCollection(Document):
    name: str
    age: int

    bosc_database_path = "test_db"


class SampleWithCollection(SampleWithoutCollection):
    bosc_collection_name = "test_collection"


class TestCollectionName:
    def test_collection_name_none(self):
        sample = SampleWithoutCollection(name="John", age=25)
        assert sample.bosc_collection_name is None
        assert (
            sample.get_collection().collection_name
            == SampleWithoutCollection.__name__
        )

    def test_collection_name_populated(self):
        sample = SampleWithCollection(name="John", age=25)
        assert sample.bosc_collection_name == "test_collection"
        assert sample.get_collection().collection_name == "test_collection"
