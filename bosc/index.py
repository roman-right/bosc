import re
from enum import Enum


class IndexType(str, Enum):
    PATH = "path"
    UNIQUE = "unique"


class Index:
    def __init__(
        self, value, index_type: IndexType = IndexType.PATH, name=None
    ):
        self.index_type = index_type
        self.value = value
        self.name = name or self.generate_name()

    def generate_name(self):
        return f"idx_{self.index_type.value}_{self.value}".replace(".", "_")

    def index_signature(self):
        return self.index_type, self.value

    def __eq__(self, other):
        return self.index_signature() == other.index_signature()

    def __str__(self):
        return f"Index({self.index_type}, {self.value}, {self.name})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def extract_type(sql: str) -> IndexType:
        if "UNIQUE" in sql:
            return IndexType.UNIQUE
        return IndexType.PATH

    @staticmethod
    def extract_value(sql: str) -> str:
        if "json_extract" in sql:
            match = re.search(r"json_extract\(.+, '\$\.(.+)'", sql)
            return match.group(1) if match else ""
        raise ValueError("Invalid index SQL")
