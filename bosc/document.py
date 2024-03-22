from typing import ClassVar, List, Optional, TypeVar
from uuid import uuid4

from pydantic import UUID4, BaseModel, Field

from bosc.collection import Collection, OnConflict, OrderDirection
from bosc.database import Database
from bosc.encoder import get_dict
from bosc.fields import ExpressionField
from bosc.index import Index, IndexType
from bosc.query.base import Query, UpdateOperation
from bosc.query.find.comparison import Eq
from bosc.query.find.logical import And

BaseModelMetaclass = type(BaseModel)

DocType = TypeVar("DocType", bound="Document")


class CombinedMeta(BaseModelMetaclass):
    def __getattr__(cls, item):
        if item in cls.__annotations__:
            return ExpressionField(item)
        raise AttributeError


# Adjusting MyClass to use the combined metaclass
class Document(BaseModel, metaclass=CombinedMeta):
    id: UUID4 = Field(default_factory=uuid4)
    bosc_database_path: ClassVar[Optional[str]] = None
    bosc_collection_name: ClassVar[Optional[str]] = None
    bosc_indexes: ClassVar[List[Index]] = Field(default_factory=list)
    bosc_json_encoders: ClassVar[Optional[dict]] = None
    bosc_database: ClassVar[Optional[Database]] = None

    @classmethod
    def get_collection(cls) -> Collection:
        if cls.bosc_collection_name is None:
            return cls.get_database()[cls.__name__]
        return cls.get_database()[cls.bosc_collection_name]

    @classmethod
    def get_database(cls) -> Database:
        if cls.bosc_database is None:
            if cls.bosc_database_path is None:
                raise ValueError("Database path is not set")
            cls.bosc_database = Database(cls.bosc_database_path)
        return cls.bosc_database

    @classmethod
    def _get_indexes_to_sync(cls):
        id_index = Index("id", IndexType.UNIQUE)
        additional_indexes = cls.bosc_indexes
        return [id_index] + additional_indexes

    @classmethod
    def sync_indexes(cls):
        cls.get_collection().sync_indexes(cls._get_indexes_to_sync())

    # ENTITY METHODS

    def insert(self, on_conflict: OnConflict = OnConflict.RAISE) -> "DocType":
        document_data = get_dict(self)
        result = self.get_collection().insert(document_data, on_conflict)
        self.id = result["id"]
        return self

    def save(self) -> "DocType":
        return self.insert(OnConflict.REPLACE)

    def delete(self) -> None:
        self.get_collection().delete(Eq("id", self.id))

    # CLASS METHODS
    @classmethod
    def insert_many(
        cls, documents, on_conflict: OnConflict = OnConflict.RAISE
    ) -> None:
        document_data = [get_dict(document) for document in documents]
        cls.get_collection().insert_many(document_data, on_conflict)

    @classmethod
    def get(cls, id) -> Optional["DocType"]:
        data = cls.get_collection().get(id)
        if data is None:
            return None
        return cls.model_validate(data)

    @classmethod
    def find(
        cls,
        *queries,
        order_by: str = None,
        order_direction: OrderDirection = OrderDirection.ASC,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List["DocType"]:
        query = (
            And(*queries)
            if len(queries) > 1
            else queries[0]
            if len(queries) == 1
            else None
        )
        result = cls.get_collection().find(
            query, order_by, order_direction, offset, limit
        )
        return [cls.model_validate(data) for data in result]

    @classmethod
    def find_one(
        cls,
        *queries,
        order_by: str = None,
        order_direction: OrderDirection = OrderDirection.ASC,
    ) -> Optional["DocType"]:
        query = (
            And(*queries)
            if len(queries) > 1
            else queries[0]
            if len(queries) == 1
            else None
        )
        result = cls.get_collection().find_one(
            query, order_by, order_direction
        )
        if result is None:
            return None
        return cls.model_validate(result)

    @classmethod
    def count(cls, *queries) -> int:
        if len(queries) == 0:
            return cls.get_collection().count()
        elif len(queries) == 1:
            return cls.get_collection().count(queries[0])
        else:
            return cls.get_collection().count(And(*queries))

    @staticmethod
    def _extract_queries(queries):
        find_queries = [query for query in queries if isinstance(query, Query)]
        if len(find_queries) == 0:
            find_query = None
        elif len(find_queries) == 1:
            find_query = find_queries[0]
        else:
            find_query = And(*find_queries)
        update_queries = [
            query for query in queries if isinstance(query, UpdateOperation)
        ]
        return find_query, update_queries

    @classmethod
    def update(cls, *queries) -> None:
        find_query, update_queries = cls._extract_queries(queries)
        cls.get_collection().update(find_query, *update_queries)

    @classmethod
    def update_one(cls, *queries) -> None:
        find_query, update_queries = cls._extract_queries(queries)
        cls.get_collection().update_one(find_query, *update_queries)

    @classmethod
    def delete_many(cls, *queries) -> None:
        if len(queries) == 0:
            cls.get_collection().delete()
        elif len(queries) == 1:
            cls.get_collection().delete(queries[0])
        else:
            cls.get_collection().delete(And(*queries))
