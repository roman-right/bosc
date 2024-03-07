import ipaddress
import pathlib
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List
from uuid import UUID, uuid4

from pydantic import BaseModel, SecretStr

from bosc import Document
from bosc.encoder import get_dict


class ExampleEnum(Enum):
    OPTION_ONE = "Option 1"
    OPTION_TWO = "Option 2"


class NestedModel(BaseModel):
    name: str = "Nested Name"
    count: int = 10


class FieldyModel(Document):
    enum_field: ExampleEnum = ExampleEnum.OPTION_ONE
    bytes_field: bytes = b"test"
    datetime_field: datetime = datetime(
        2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc
    )
    date_field: date = date(2023, 1, 1)
    timedelta_field: timedelta = timedelta(days=1)
    uuid_field: UUID = uuid4()
    ip_v4_address: ipaddress.IPv4Address = ipaddress.IPv4Address("192.168.1.1")
    ip_v6_address: ipaddress.IPv6Address = ipaddress.IPv6Address("::1")
    path_field: pathlib.PurePath = pathlib.Path("/test/path")
    secret_str: SecretStr = SecretStr("secret_string")
    nested_model: NestedModel = NestedModel()
    string_list: List[str] = ["item1", "item2"]
    nested_model_list: List[NestedModel] = [
        NestedModel(),
        NestedModel(name="Another Nested", count=20),
    ]
    dictionary: Dict[str, Any] = {
        "key": "value",
        "enum": ExampleEnum.OPTION_TWO,
        "nested": NestedModel(),
    }

    bosc_database_path = "test_db"


class TestEncoder:
    def test_encode(self):
        fieldy_model = FieldyModel()
        result = get_dict(fieldy_model)
        assert isinstance(result["id"], str)
        result.pop("id")
        assert isinstance(result["uuid_field"], str)
        result.pop("uuid_field")
        assert result == {
            "date_field": "2023-01-01",
            "datetime_field": 1672574400.0,
            "dictionary": {
                "enum": "Option 2",
                "key": "value",
                "nested": {"count": 10, "name": "Nested Name"},
            },
            "enum_field": "Option 1",
            "ip_v4_address": "192.168.1.1",
            "ip_v6_address": "::1",
            "nested_model": {"count": 10, "name": "Nested Name"},
            "nested_model_list": [
                {"count": 10, "name": "Nested Name"},
                {"count": 20, "name": "Another Nested"},
            ],
            "path_field": "/test/path",
            "secret_str": "secret_string",
            "string_list": ["item1", "item2"],
            "timedelta_field": 86400.0,
            "bytes_field": "test",
        }

        fieldy_model.insert()
        result = FieldyModel.get(fieldy_model.id)

        assert result.date_field == date(2023, 1, 1)
        assert result.datetime_field == datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc
        )
        assert result.timedelta_field == timedelta(days=1)
        assert result.uuid_field == fieldy_model.uuid_field
        assert result.ip_v4_address == ipaddress.IPv4Address("192.168.1.1")
        assert result.ip_v6_address == ipaddress.IPv6Address("::1")
        assert result.path_field == pathlib.Path("/test/path")
        assert result.secret_str == SecretStr("secret_string")
        assert result.nested_model == NestedModel()
        assert result.string_list == ["item1", "item2"]
        assert result.nested_model_list == [
            NestedModel(),
            NestedModel(name="Another Nested", count=20),
        ]
        assert result.dictionary == {
            "key": "value",
            "enum": "Option 2",
            "nested": {"name": "Nested Name", "count": 10},
        }
