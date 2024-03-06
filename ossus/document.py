from typing import ClassVar, Optional, List
from uuid import uuid4

from pydantic import BaseModel, UUID4, Field

from ossus.collection import Collection, OnConflict
from ossus.database import Database
from ossus.encoder import get_dict
from ossus.fields import ExpressionField
from ossus.index import Index
from ossus.queries.find.comparison import Eq
from ossus.queries.find.logical import And

BaseModelMetaclass = type(BaseModel)


class CombinedMeta(BaseModelMetaclass):
    def __getattr__(cls, item):
        if item in cls.__annotations__:
            return ExpressionField(item)
        raise AttributeError


# Adjusting MyClass to use the combined metaclass
class Document(BaseModel, metaclass=CombinedMeta):
    id: UUID4 = Field(default_factory=uuid4)
    pear_indexes: ClassVar[List[Index]] = Field(default_factory=list)
    pear_database_path: ClassVar[Optional[str]] = None
    pear_database: ClassVar[Optional[Database]] = None
    pear_collection: ClassVar[Optional[str]] = None
    pear_json_encoders: ClassVar[Optional[dict]] = None

    @classmethod
    def get_collection(cls) -> Collection:
        if cls.pear_collection is None:
            return cls.get_database()[cls.__class__.__name__]
        return cls.get_database()[cls.pear_collection]

    @classmethod
    def get_database(cls) -> Database:
        if cls.pear_database is None:
            if cls.pear_database_path is None:
                raise ValueError("Database path is not set")
            cls.pear_database = Database(cls.pear_database_path)
        return cls.pear_database

    # ENTITY METHODS

    def insert(self, on_conflict: OnConflict = OnConflict.RAISE):
        document_data = get_dict(self)
        result = self.get_collection().insert(document_data, on_conflict)
        self.id = result["id"]
        return self

    def save(self):
        self.insert(OnConflict.REPLACE)

    def delete(self):
        self.get_collection().delete(Eq("id", self.id))

    # CLASS METHODS

    @classmethod
    def get(cls, id):
        data = cls.get_collection().get(id)
        return cls.model_validate(data)

    @classmethod
    def find(cls, *queries):
        if len(queries) == 0:
            result = cls.get_collection().find()
        elif len(queries) == 1:
            result = cls.get_collection().find(queries[0])
        else:
            result = cls.get_collection().find(And(*queries))
        return [cls.model_validate(data) for data in result]

    @classmethod
    def find_one(cls, *queries):
        if len(queries) == 0:
            result = cls.get_collection().find_one()
        elif len(queries) == 1:
            result = cls.get_collection().find_one(queries[0])
        else:
            result = cls.get_collection().find_one(And(*queries))
        return cls.model_validate(result)

    @classmethod
    def count(cls, *queries):
        if len(queries) == 0:
            return cls.get_collection().count()
        elif len(queries) == 1:
            return cls.get_collection().count(queries[0])
        else:
            return cls.get_collection().count(And(*queries))

    @classmethod
    def update(cls, query, update):
        cls.get_collection().update(query, update)

    @classmethod
    def update_one(cls, query, update):
        cls.get_collection().update_one(query, update)

    @classmethod
    def delete_many(cls, *queries):
        if len(queries) == 0:
            cls.get_collection().delete()
        elif len(queries) == 1:
            cls.get_collection().delete(queries[0])
        else:
            cls.get_collection().delete(And(*queries))
