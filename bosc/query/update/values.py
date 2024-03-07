from bosc.encoder import encode
from bosc.query.base import UpdateOperation


class Set(UpdateOperation):
    def __init__(self, field, value):
        self.field = encode(field)
        self.value = encode(value)

    def to_sql_update(self):
        return f"json_set(data, '$.{self.field}', ?)", [self.value]


class Inc(UpdateOperation):
    def __init__(self, field, increment_by=1):
        self.field = field
        self.increment_by = increment_by

    def to_sql_update(self):
        return (
            f"json_set(data, '$.{self.field}', json_extract(data, '$.{self.field}') + ?)",
            [self.increment_by],
        )


class Now(UpdateOperation):
    def __init__(self, field):
        self.field = field

    def to_sql_update(self):
        return (
            f"json_set(data, '$.{self.field}', CAST(strftime('%s', 'now') AS INTEGER))",
            [],
        )


class RemoveField(UpdateOperation):
    def __init__(self, field):
        self.field = field

    def to_sql_update(self):
        return f"json_remove(data, '$.{self.field}')", []
