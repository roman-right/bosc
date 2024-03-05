from ossus.queries.base import Query


class ComparisonQuery(Query):
    def __init__(self, field, value):
        self.field = field
        self.value = value


class Eq(ComparisonQuery):
    def to_sql(self):
        return f"json_extract(data, '$.{self.field}') = ?", [self.value]


class Neq(ComparisonQuery):
    def to_sql(self):
        return f"json_extract(data, '$.{self.field}') != ?", [self.value]


class Gt(ComparisonQuery):
    def to_sql(self):
        return f"json_extract(data, '$.{self.field}') > ?", [self.value]


class Gte(ComparisonQuery):
    def to_sql(self):
        return f"json_extract(data, '$.{self.field}') >= ?", [self.value]


class Lt(ComparisonQuery):
    def to_sql(self):
        return f"json_extract(data, '$.{self.field}') < ?", [self.value]


class Lte(ComparisonQuery):
    def to_sql(self):
        return f"json_extract(data, '$.{self.field}') <= ?", [self.value]


class In(ComparisonQuery):
    def to_sql(self):
        placeholders = ', '.join(['?'] * len(self.value))
        return f"json_extract(data, '$.{self.field}') IN ({placeholders})", self.value


class Nin(ComparisonQuery):
    def to_sql(self):
        placeholders = ', '.join(['?'] * len(self.value))
        return f"json_extract(data, '$.{self.field}') NOT IN ({placeholders})", self.value
