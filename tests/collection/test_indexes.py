from ossus.index import Index, IndexType


class TestIndexes:
    def test_create_index_by_path(self, collection, conn):
        collection.create_index(Index(IndexType.PATH, "address.city"))
        indxes = collection.get_indexes()
        assert len(indxes) == 2
        assert indxes[0].name == "idx_path_address_city"
        assert indxes[0].index_type == IndexType.PATH
        assert indxes[0].value == "address.city"

        get_index_sql = "SELECT sql FROM sqlite_master WHERE type='index'"
        cursor = conn.cursor()
        cursor.execute(get_index_sql)
        result = cursor.fetchall()
        assert result[1] == (
            """CREATE INDEX "idx_path_address_city" ON "test_collection" (json_extract(data, '$.address.city'))""",
        )

    def test_drop_index_by_name(self, collection, conn):
        collection.create_index(Index(IndexType.PATH, "address.city"))
        indxes = collection.get_indexes()
        assert len(indxes) == 2

        collection.drop_index("idx_path_address_city")
        indxes = collection.get_indexes()
        print(indxes)
        assert len(indxes) == 1
        assert indxes[0].name == "idx_path_id"

        get_index_sql = "SELECT sql FROM sqlite_master WHERE type='index'"
        cursor = conn.cursor()
        cursor.execute(get_index_sql)
        result = cursor.fetchall()
        assert result[0] == (
            """CREATE INDEX "idx_path_id" ON "test_collection" (json_extract(data, '$.id'))""",
        )

    def test_drop_index_by_index(self, collection, conn):
        collection.create_index(Index(IndexType.PATH, "address.city"))
        indxes = collection.get_indexes()
        assert len(indxes) == 2

        collection.drop_index(
            Index(
                IndexType.PATH,
                "address.city",
            )
        )
        indxes = collection.get_indexes()
        assert len(indxes) == 1
        assert indxes[0].name == "idx_path_id"

        get_index_sql = "SELECT sql FROM sqlite_master WHERE type='index'"
        cursor = conn.cursor()
        cursor.execute(get_index_sql)
        result = cursor.fetchall()
        assert result[0] == (
            """CREATE INDEX "idx_path_id" ON "test_collection" (json_extract(data, '$.id'))""",
        )

    def test_sync_indexes(self, collection, conn):
        collection.create_index(Index(IndexType.PATH, "address.city"))
        collection.create_index(Index(IndexType.PATH, "address.state"))
        indxes = collection.get_indexes()
        assert len(indxes) == 3

        collection.sync_indexes(
            [
                Index(IndexType.PATH, "id"),
                Index(IndexType.PATH, "address.city"),
                Index(IndexType.PATH, "age"),
            ]
        )

        indxes = collection.get_indexes()
        assert len(indxes) == 3
        assert indxes[0].name == "idx_path_address_city"
        assert indxes[1].name == "idx_path_age"
        assert indxes[2].name == "idx_path_id"
