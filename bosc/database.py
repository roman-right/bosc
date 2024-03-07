import sqlite3
from contextlib import contextmanager
from pathlib import Path

from bosc.collection import Collection


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.connection.execute("PRAGMA journal_mode=WAL;")
        self.name = Path(db_path).stem

    def __getattr__(self, collection_name: str):
        return Collection(collection_name, self.connection, self)

    def __getitem__(self, collection_name: str):
        return self.__getattr__(collection_name)

    @contextmanager
    def _cursor(self):
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def drop_collection(self, collection_name: str):
        with self._cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {collection_name}")
            self.connection.commit()

    def drop_all_collections(self):
        with self._cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            result = cursor.fetchall()
            for table in result:
                cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
            self.connection.commit()

    def drop_all_indexes(self):
        with self._cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            result = cursor.fetchall()
            for index in result:
                cursor.execute(f"DROP INDEX {index[0]}")
            self.connection.commit()
