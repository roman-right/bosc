from bosc.query.base import Query


class LogicalQuery(Query):
    def __init__(self, *queries):
        self.queries = queries


class And(LogicalQuery):
    def to_sql(self):
        parts = []
        params = []
        for query in self.queries:
            part, param = query.to_sql()
            parts.append(f"({part})")
            params.extend(param)
        return " AND ".join(parts), params


class Or(LogicalQuery):
    def to_sql(self):
        parts = []
        params = []
        for query in self.queries:
            part, param = query.to_sql()
            parts.append(f"({part})")
            params.extend(param)
        return " OR ".join(parts), params
