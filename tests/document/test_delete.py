from tests.document.models import Sample


class TestDelete:
    def test_delete(self, samples):
        sample = Sample.find_one(Sample.name == "John")
        sample.delete()
        result = Sample.find(Sample.name == "John")
        assert len(result) == 1
        assert result[0].age == 40

    def test_delete_many(self, samples):
        Sample.delete_many(Sample.name == "John")
        result = Sample.find(Sample.name == "John")
        assert len(result) == 0
        result = Sample.find()
        assert len(result) == 2
        assert result[0].name == "Jane"
        assert result[1].name == "Jack"
