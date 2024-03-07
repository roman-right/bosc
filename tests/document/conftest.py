import pytest

from tests.document.models import Sample


@pytest.fixture
def samples():
    to_insert = [
        Sample(name="John", age=25),
        Sample(name="Jane", age=30),
        Sample(name="Jack", age=35),
        Sample(name="John", age=40),
    ]
    for sample in to_insert:
        sample.insert()
    return to_insert
