from pydantic import BaseModel

from ossus.fields import ExpressionField

BaseModelMetaclass = type(BaseModel)


class CombinedMeta(BaseModelMetaclass):
    def __getattr__(cls, item):
        if item in cls.__annotations__:
            return ExpressionField(item)
        raise AttributeError


# Adjusting MyClass to use the combined metaclass
class Document(BaseModel, metaclass=CombinedMeta):

    def insert(self, db):
        db.insert(self)
