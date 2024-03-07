![Bosc](logo/big.svg)

# Bosc

## Introduction
The Bosc Document Store provides a lightweight, Pythonic interface for interacting with JSON documents stored in an SQLite database. It supports basic CRUD operations, querying with various conditions, and working with indexes for improved query performance.

Bosc uses Pydantic to 


## Installation
Before you begin, ensure you have Python installed on your system. This document store requires Python 3.8 or newer.

To use the Bosc Document Store, you'll first need to include it in your Python project. If the Bosc package is available via a package manager (like pip), you can install it using:

```
pip install bosc
```

## Quickstart

### Setting Up Your Database

```python
from bosc.database import Database

# Initialize the database. This creates a new SQLite file if it doesn't exist.
db = Database("my_database.db")
```

### Defining a Document Model

```python
from bosc import Document


class User(Document):
    name: str
    age: int

    # Specify the database path for this document
    bosc_database_path = "my_database.db"
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
class User(Document):
    name: str
    age: int

    bosc_indexes = [
        Index("name", IndexType.PATH),
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
from bosc import And, Or, Eq, Gt

# Find users named John Doe over 30 years old
users = User.find(And(User.name == "John Doe", User.age > 30))

# Find users either named Alice or younger than 25
users = User.find(Or(User.name == "Alice", User.age < 25))

# Order users by age
users = User.find(order_by="age", order_direction=OrderDirection.ASC)
```
