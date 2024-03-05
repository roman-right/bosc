from pydantic import BaseModel


class DocumentSettings(BaseModel):
    indexes: list = []