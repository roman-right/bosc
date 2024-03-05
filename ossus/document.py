from typing import ClassVar, Optional, List
from uuid import uuid4

from pydantic import BaseModel, UUID4, Field

from ossus.database import Database
from ossus.fields import ExpressionField
from ossus.index import Index

BaseModelMetaclass = type(BaseModel)


class CombinedMeta(BaseModelMetaclass):
    def __getattr__(cls, item):
        if item in cls.__annotations__:
            return ExpressionField(item)
        raise AttributeError


# Adjusting MyClass to use the combined metaclass
class Document(BaseModel, metaclass=CombinedMeta):
    id: UUID4 = Field(default_factory=uuid4)
    _indexes: ClassVar[List[Index]] = Field(default_factory=list)
    _database_path: ClassVar[Optional[str]] = None
    _database: ClassVar[Optional[Database]] = None
    _collection: ClassVar[Optional[str]] = None

    @property
    def collection(self):
        if self._collection is None:
            return self.database[self.__class__.__name__]
        return self.database[self._collection]

    @property
    def database(self):
        if self._database is None:
            if self._database_path is None:
                raise ValueError("Database path is not set")
            self._database = Database(self._database_path)
        return self._database

    def insert(self):
        result = self.collection.insert(self)
        self.id = result["id"]
        return self

    def get(self, id):
        data = self.collection.get(id)
        return self.model_validate_json(data)
