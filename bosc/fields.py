from bosc.query.find.comparison import Eq, Gt, Gte, Lt, Lte, Neq


class ExpressionField:
    def __init__(self, attr_name):
        self.attr_name = attr_name

    def __getattr__(self, item):
        if item.endswith("__") and item.startswith("__"):
            raise AttributeError
        return ExpressionField(f"{self.attr_name}.{item}")

    def __getitem__(self, item):
        return getattr(self, item)

    def __eq__(self, other):
        return Eq(self.attr_name, other)

    def __ne__(self, other):
        return Neq(self.attr_name, other)

    def __gt__(self, other):
        return Gt(self.attr_name, other)

    def __ge__(self, other):
        return Gte(self.attr_name, other)

    def __lt__(self, other):
        return Lt(self.attr_name, other)

    def __le__(self, other):
        return Lte(self.attr_name, other)
