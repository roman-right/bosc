class Query:
    def to_sql(self):
        raise NotImplementedError(
            "This method should be implemented by subclasses."
        )


class UpdateOperation:
    def to_sql_update(self):
        raise NotImplementedError(
            "This method should be implemented by subclasses."
        )
