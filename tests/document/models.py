from bosc import Document


class Sample(Document):
    name: str
    age: int

    bosc_database_path = "test_db"
