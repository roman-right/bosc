from ossus import Document


class Sample(Document):
    name: str
    age: int

    pear_database_path = "test.db"


class TestDocumentInsert:
    def test_insert(self):
        sample = Sample(name="John", age=25)
        sample.insert()
        assert sample.id is not None
        result = Sample.get(sample.id)
        assert result.name == "John"
        assert result.age == 25
