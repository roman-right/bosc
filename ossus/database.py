import os
import sqlite3
from pathlib import Path

from ossus.collection import Collection


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.name = Path(db_path).stem

    def __getattr__(self, collection_name: str):
        return Collection(collection_name, self.conn, self)

    def __getitem__(self, collection_name: str):
        return self.__getattr__(collection_name)

    def drop_collection(self, collection_name: str):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (collection_name,))
        result = cursor.fetchone()
        if not result:
            return  # collection does not exist
        self.conn.execute(f'DROP TABLE {collection_name}')
        self.conn.commit()

    def drop_all_collections(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        result = cursor.fetchall()
        for table in result:
            self.conn.execute(f'DROP TABLE {table[0]}')
        self.conn.commit()

    def drop_all_indexes(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        result = cursor.fetchall()
        for index in result:
            self.conn.execute(f'DROP INDEX {index[0]}')
        self.conn.commit()

    def close(self):
        self.conn.close()
