from bosc import Set
from tests.document.models import Sample


class TestUpdate:
    def test_update(self, samples):
        Sample.update(Sample.name == "John", Set("age", 50))
        result = Sample.find(Sample.name == "John")
        assert len(result) == 2
        assert result[0].age == 50
        assert result[1].age == 50

    def test_update_one(self, samples):
        Sample.update_one(Sample.name == "John", Set("age", 50))
        result = Sample.find(Sample.name == "John")
        assert len(result) == 2
        assert result[0].age == 50
        assert result[1].age == 40
