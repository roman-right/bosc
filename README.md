[![Bosc](https://raw.githubusercontent.com/roman-right/bosc/main/logo/logo.svg)](https://github.com/roman-right/bosc)

# Bosc

[![pypi](https://img.shields.io/pypi/v/bosc.svg)](https://pypi.python.org/pypi/bosc)

Bosc is a document store that provides an easy, Pythonic interface for handling documents stored on the local file system. It utilizes the full power of SQLite for storing and querying documents and employs Pydantic as the parsing and validation engine, ensuring it works seamlessly within the modern Python ecosystem, including FastAPI.


> ⚠️ Bosc is not an ORM for SQLite. It is a document-oriented store that uses SQLite as a backend. It is not designed to replace traditional SQL databases, but rather to provide a simple, Pythonic interface for working with flexible documents.

## Installation
Before you begin, ensure you have Python installed on your system. This document store requires Python 3.8 or newer.

You can install Bosc using pip:

```shell
pip install bosc
```

## Quickstart

### Defining a Document Model

```python
from bosc import Document


class User(Document):
    name: str
    age: int

    # Specify the database path for this document
    bosc_database_path = "my_database.db"
    
    # Specify the collection name for this document (Optional)
    bosc_collection_name = "users"
```

### Inserting Documents
```python
user = User(name="John Doe", age=30)
user.insert()  # Insert the document into the database

# Inserting multiple documents
User.insert_many([
    User(name="Jane Doe", age=25),
    User(name="Alice", age=35),
])
```

### Querying Documents
```python
# Find all users
users = User.find()

# Find users with specific criteria
johns = User.find(User.name == "John Doe")
young_users = User.find(User.age < 30)

# Find users with multiple criteria
users = User.find(User.name == "John Doe", User.age > 30)

# Find having order by
users = User.find(
    User.name == "John Doe", 
    order_by="age", 
    order_direction=OrderDirection.ASC
)

# Find with limit
users = User.find(User.name == "John Doe", limit=1)

# Find with offset
users = User.find(User.name == "John Doe", offset=1)

# Find a single user
jane = User.find_one(User.name == "Jane Doe")
```

### Updating Documents
```python
# Update all users named John Doe to have age 31
User.update(User.name == "John Doe", Set("age", 31))

# Update a single document
john = User.find_one(User.name == "John Doe")
john.age = 32
john.save()  # This uses the OnConflict.REPLACE strategy
```

### Deleting Documents
```python
# Delete a specific user
jane.delete()

# Delete all users named Alice
User.delete_many(User.name == "Alice")
```

## Advanced Usage

### Working with Indexes
To improve query performance, you can define indexes on your document fields.

```python
from bosc import Document, Index, IndexType

class User(Document):
    name: str
    age: int

    bosc_indexes = [
        Index("name", IndexType.UNIQUE),
        Index("age", IndexType.PATH),
    ]
    bosc_database_path = "my_database.db"

# Sync indexes with the database
User.sync_indexes()
```

This will create indexes on the `name` and `age` fields of the `User` documents, assuming `Index` and `IndexType` are properly defined and imported from the `bosc` package.

### Complex Queries
Leverage the full power of queries with complex conditions and ordering.

```python
from bosc import And, Or
from bosc import OrderDirection

# Find users named John Doe over 30 years old
users = User.find(And(User.name == "John Doe", User.age > 30))

# Find users either named Alice or younger than 25
users = User.find(Or(User.name == "Alice", User.age < 25))

# Order users by age
users = User.find(order_by="age", order_direction=OrderDirection.ASC)
```
