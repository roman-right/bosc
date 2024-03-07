import json
import logging
import uuid
from contextlib import contextmanager
from enum import Enum
from typing import Dict, List, Optional, Union

from bosc.index import Index, IndexType
from bosc.query.base import Query, UpdateOperation
from bosc.query.find.comparison import Eq

logger = logging.getLogger(__name__)


class OrderDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class OnConflict(str, Enum):
    REPLACE = "REPLACE"
    IGNORE = "IGNORE"
    RAISE = "RAISE"


class Collection:
    def __init__(self, collection_name, connection, database):
        self.connection = connection
        self.connection.execute("PRAGMA journal_mode=WAL;")
        self.collection_name = collection_name
        self.database = database
        self._create_table()

    @contextmanager
    def _cursor(self):
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _create_table(self):
        with self._cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {self.collection_name} (id INTEGER PRIMARY KEY, data JSON)"
            )
            self.connection.commit()
        self.create_index(Index("id", IndexType.UNIQUE))

    def insert(
        self, document: dict, on_conflict: OnConflict = OnConflict.RAISE
    ) -> Dict:
        if not isinstance(document, dict):
            raise ValueError("Document must be a dictionary")
        if "id" not in document:
            document["id"] = uuid.uuid4().hex

        with self._cursor() as cursor:
            if on_conflict == OnConflict.REPLACE:
                cursor.execute(
                    f"INSERT OR REPLACE INTO {self.collection_name} (data) "
                    f"VALUES (json(?)) "
                    f"RETURNING data",
                    [json.dumps(document)],
                )

            elif on_conflict == OnConflict.IGNORE:
                cursor.execute(
                    f"INSERT OR IGNORE INTO {self.collection_name} (data) "
                    f"VALUES (json(?)) "
                    f"RETURNING data",
                    [json.dumps(document)],
                )
            else:
                try:
                    cursor.execute(
                        f"INSERT INTO {self.collection_name} (data) "
                        f"VALUES (json(?)) "
                        f"RETURNING data",
                        [json.dumps(document)],
                    )
                except Exception as e:
                    self.connection.rollback()
                    raise e
            inserted_document = cursor.fetchone()
            self.connection.commit()

        if inserted_document is None and on_conflict == OnConflict.IGNORE:
            logger.warning(
                f"As the document with id {document['id']} already exists and on_conflict is set to IGNORE, "
                f"the document was not inserted. Returning the existing document. The operation is not atomic."
            )
            return self.get(document["id"])
        return json.loads(inserted_document[0])

    def insert_many(
        self, documents: list, on_conflict: OnConflict = OnConflict.RAISE
    ) -> None:
        documents_as_json = [(json.dumps(doc),) for doc in documents]
        with self._cursor() as cursor:
            if on_conflict == OnConflict.REPLACE:
                cursor.executemany(
                    f"INSERT OR REPLACE INTO {self.collection_name} (data) VALUES (json(?))",
                    documents_as_json,
                )
            elif on_conflict == OnConflict.IGNORE:
                cursor.executemany(
                    f"INSERT OR IGNORE INTO {self.collection_name} (data) VALUES (json(?))",
                    documents_as_json,
                )
            else:
                try:
                    cursor.executemany(
                        f"INSERT INTO {self.collection_name} (data) VALUES (json(?))",
                        documents_as_json,
                    )
                except Exception as e:
                    self.connection.rollback()
                    raise e
            self.connection.commit()

    def find(
        self,
        query: Optional[Query] = None,
        order_by: str = None,
        order_direction: OrderDirection = OrderDirection.ASC,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict]:
        with self._cursor() as cursor:
            if query:
                where_clause, query_val = query.to_sql()
                sql = f"SELECT data FROM {self.collection_name} WHERE {where_clause}"
            else:
                sql = f"SELECT data FROM {self.collection_name}"
                query_val = ()
            if order_by:
                sql += f" ORDER BY json_extract(data, '$.{order_by}') {order_direction.value}"
            if limit is not None:
                sql += f" LIMIT {limit}"
            elif offset is not None:
                sql += " LIMIT -1"
            if offset:
                sql += f" OFFSET {offset}"
            cursor.execute(sql, query_val)
            return [json.loads(row[0]) for row in cursor.fetchall()]

    def find_one(
        self,
        query: Optional[Query] = None,
        order_by: str = None,
        order_direction: OrderDirection = OrderDirection.ASC,
    ) -> Optional[Dict]:
        with self._cursor() as cursor:
            if query:
                where_clause, query_params = query.to_sql()
                sql = f"SELECT data FROM {self.collection_name} WHERE {where_clause}"
            else:
                sql = f"SELECT data FROM {self.collection_name}"
                query_params = []
            if order_by:
                sql += f" ORDER BY json_extract(data, '$.{order_by}') {order_direction.value}"
            sql += " LIMIT 1"

            cursor.execute(sql, query_params)
            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            return None

    def get(self, document_id) -> Optional[Dict]:
        return self.find_one(Eq("id", document_id))

    def count(self, query: Optional[Query] = None) -> int:
        with self._cursor() as cursor:
            if query:
                where_clause, query_val = query.to_sql()
                sql = f"SELECT COUNT(*) FROM {self.collection_name} WHERE {where_clause}"
            else:
                sql = f"SELECT COUNT(*) FROM {self.collection_name}"
                query_val = ()
            cursor.execute(sql, query_val)
            return cursor.fetchone()[0]

    def update(
        self, query: Optional[Query] = None, *operations: UpdateOperation
    ):
        with self._cursor() as cursor:

            # Initialize where_clause and query_params
            where_clause, query_params = ("", [])
            if query:
                # Convert query to SQL only if query is not None
                where_clause, query_params = query.to_sql()
                where_clause = f"WHERE {where_clause}"  # Prefix with WHERE only if there's a query

            # Prepare SQL update expressions and parameters from operations
            update_expressions = []
            update_params = []
            for op in operations:
                sql_part, params = op.to_sql_update()
                update_expressions.append(sql_part)
                update_params.extend(params)

            # Combine update expressions into a single SQL statement
            update_sql = ", ".join(update_expressions)
            if where_clause:
                sql = f"UPDATE {self.collection_name} SET data = {update_sql} {where_clause}"
            else:
                sql = f"UPDATE {self.collection_name} SET data = {update_sql}"

            # Execute the update query with the combined parameters (if any)
            if query_params:
                cursor.execute(sql, update_params + query_params)
            else:
                cursor.execute(sql, update_params)
            self.connection.commit()

    def update_one(
        self,
        query: Optional[Query] = None,
        *operations: UpdateOperation,
    ):
        with self._cursor() as cursor:
            # Initialize where_clause and query_params
            where_clause, query_params = ("", [])
            if query:
                # Convert query to SQL only if query is not None
                where_clause, query_params = query.to_sql()
                where_clause = f"WHERE {where_clause}"

            # Prepare SQL update expressions and parameters from operations
            update_expressions = []
            update_params = []
            for op in operations:
                sql_part, params = op.to_sql_update()
                update_expressions.append(sql_part)
                update_params.extend(params)

            # Combine update expressions into a single SQL statement
            update_sql = ", ".join(update_expressions)
            if where_clause:
                sql = f"UPDATE {self.collection_name} SET data = {update_sql} {where_clause} LIMIT 1"
            else:
                sql = f"UPDATE {self.collection_name} SET data = {update_sql} LIMIT 1"

            # Execute the update query with the combined parameters (if any)
            if query_params:
                cursor.execute(sql, update_params + query_params)
            else:
                cursor.execute(sql, update_params)
            self.connection.commit()

    def delete(self, query: Optional[Query] = None):
        with self._cursor() as cursor:
            if query:
                where_clause, query_val = query.to_sql()
                sql = (
                    f"DELETE FROM {self.collection_name} WHERE {where_clause}"
                )
            else:
                sql = f"DELETE FROM {self.collection_name}"
                query_val = ()
            cursor.execute(sql, query_val)
            self.connection.commit()

    def delete_one(self, query: Optional[Query] = None):
        with self._cursor() as cursor:
            if query:
                where_clause, query_val = query.to_sql()
                sql = f"DELETE FROM {self.collection_name} WHERE {where_clause} LIMIT 1"
            else:
                sql = f"DELETE FROM {self.collection_name} LIMIT 1"
                query_val = ()
            cursor.execute(sql, query_val)
            self.connection.commit()

    def get_indexes(self) -> List[Index]:
        with self._cursor() as cursor:
            indexes = []
            cursor.execute(
                f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='{self.collection_name}' AND sql NOT NULL order by name"
            )
            for name, sql in cursor.fetchall():
                index_type = Index.extract_type(sql)
                index_value = Index.extract_value(sql)
                indexes.append(
                    Index(
                        index_type=index_type,
                        value=index_value,
                        name=name,
                    )
                )
            return indexes

    def create_index(self, index):
        with self._cursor() as cursor:
            index_name_quoted = f'"{index.name}"'
            collection_name_quoted = f'"{self.collection_name}"'

            if index.index_type == IndexType.PATH:
                index_sql = f"CREATE INDEX IF NOT EXISTS {index_name_quoted} ON {collection_name_quoted} (json_extract(data, '$.{index.value}'))"
            elif index.index_type == IndexType.UNIQUE:
                index_sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name_quoted} ON {collection_name_quoted} (json_extract(data, '$.{index.value}'))"

            cursor.execute(index_sql)
            self.connection.commit()

    def drop_index(self, index: Union[str, Index]):
        with self._cursor() as cursor:
            if isinstance(index, str):
                cursor.execute(f"DROP INDEX IF EXISTS {index}")
            else:
                indexes = self.get_indexes()
                for idx in indexes:
                    if idx == index:
                        cursor.execute(f"DROP INDEX IF EXISTS {idx.name}")
                        break
            self.connection.commit()

    def drop_all_indexes(self):
        with self._cursor() as cursor:
            cursor.execute(
                f"DELETE FROM sqlite_master WHERE type='index' AND tbl_name='{self.collection_name}'"
            )
            self.connection.commit()

    def sync_indexes(self, indexes: List[Index]):
        existing_indexes = self.get_indexes()
        for index in indexes:
            if index not in existing_indexes:
                self.create_index(index)
        for index in existing_indexes:
            if index not in indexes:
                self.drop_index(index)
