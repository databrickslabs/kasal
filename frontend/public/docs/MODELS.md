# SQLAlchemy Models Guide

This document provides comprehensive documentation for SQLAlchemy models in our backend architecture.

## Table of Contents

- [Overview](#overview)
- [Structure and Conventions](#structure-and-conventions)
- [Base Model Configuration](#base-model-configuration)
- [Model Relationships](#model-relationships)
- [Common Field Types](#common-field-types)
- [Indexing Strategy](#indexing-strategy)
- [Lifecycle Events](#lifecycle-events)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Overview

SQLAlchemy models represent database tables and are a core component of the **Database Layer** in our architecture. They:

- Define the data structure and relationships in the database
- Map Python objects to database records
- Provide a type-safe interface for working with data
- Handle database constraints and validations

Models are stored in the `src/models/` directory, with each model typically in its own file.

## Structure and Conventions

### File Organization

```
src/models/
├── __init__.py           # Exposes models for easier imports
├── item.py               # Models related to items
├── user.py               # Models related to users
└── other_domain.py       # Other domain-specific models
```

### Naming Conventions

- Model class names: **PascalCase**
- Table names: **snake_case** (auto-generated from class name)
- Column names: **snake_case**
- Primary key: `id` (standard across all models)
- Foreign keys: `{table_name}_id` (e.g., `user_id`)

## Base Model Configuration

All models inherit from a common `Base` class which provides consistent behavior:

```python
# src/db/base.py
from typing import Any

from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all database models."""
    
    id: Any
    
    # Generate __tablename__ automatically based on class name
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()
```

## Model Relationships

SQLAlchemy supports various relationship types, configured using the `relationship` function.

### One-to-Many Relationship

```python
# Parent model (One)
class User(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    
    # Define relationship
    posts: Mapped[List["Post"]] = relationship("Post", back_populates="user")

# Child model (Many)
class Post(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    
    # Foreign key
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    
    # Define relationship
    user: Mapped["User"] = relationship("User", back_populates="posts")
```

### Many-to-Many Relationship

```python
# Association table (no model, just a table)
tag_item_association = Table(
    "tag_item_association",
    Base.metadata,
    Column("tag_id", Integer, ForeignKey("tag.id"), primary_key=True),
    Column("item_id", Integer, ForeignKey("item.id"), primary_key=True),
)

# First entity
class Tag(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True)
    
    # Define relationship
    items: Mapped[List["Item"]] = relationship(
        "Item", 
        secondary=tag_item_association,
        back_populates="tags"
    )

# Second entity
class Item(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    
    # Define relationship
    tags: Mapped[List["Tag"]] = relationship(
        "Tag", 
        secondary=tag_item_association,
        back_populates="items"
    )
```

### One-to-One Relationship

```python
class User(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    
    # Define one-to-one relationship
    profile: Mapped["UserProfile"] = relationship(
        "UserProfile", 
        back_populates="user", 
        uselist=False
    )

class UserProfile(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    bio: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    
    # Foreign key
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), unique=True)
    
    # Define relationship back to User
    user: Mapped["User"] = relationship("User", back_populates="profile")
```

## Common Field Types

SQLAlchemy supports various field types, here are the most common ones:

| Python Type | SQLAlchemy Type | Description |
|-------------|-----------------|-------------|
| `int` | `Integer` | Integer values |
| `float` | `Float` | Floating point values |
| `str` | `String(length)` | Variable length strings with max length |
| `str` | `Text` | Unlimited length strings |
| `bool` | `Boolean` | True/False values |
| `datetime` | `DateTime` | Date and time values |
| `date` | `Date` | Date values without time |
| `timedelta` | `Interval` | Time intervals |
| `Decimal` | `Numeric` | Precise decimal values |
| `bytes` | `LargeBinary` | Binary data (files, etc.) |
| `dict` | `JSON` | JSON data |
| `enum.Enum` | `Enum` | Enumeration values |
| `UUID` | `UUID` | UUID values |

Example usage:

```python
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import DateTime, String, Boolean, Text
from sqlalchemy.orm import Mapped, mapped_column

class Product(Base):
    id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    price: Mapped[float]
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow, 
        onupdate=datetime.utcnow
    )
```

## Indexing Strategy

Indexes improve query performance. Common index types:

### Simple Indexes

```python
class User(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), index=True, unique=True)
    username: Mapped[str] = mapped_column(String(50), index=True, unique=True)
```

### Composite Indexes

```python
class Order(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("product.id"))
    quantity: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(DateTime)
    
    # Create a composite index on user_id and created_at
    __table_args__ = (
        Index('ix_order_user_id_created_at', 'user_id', 'created_at'),
    )
```

### When to Use Indexes

- Primary keys are automatically indexed
- Foreign keys should generally be indexed
- Columns frequently used in WHERE clauses
- Columns used in ORDER BY or GROUP BY
- Columns with high cardinality (many unique values)

## Lifecycle Events

SQLAlchemy supports event listeners for model lifecycle events:

```python
from sqlalchemy import event

# Hook for before_insert event
@event.listens_for(User, 'before_insert')
def hash_password(mapper, connection, user):
    # Hash the password before saving to database
    if user.password:
        user.password_hash = hash_password(user.password)
        user.password = None  # Don't store the plain password
```

Common events include:
- `before_insert`
- `after_insert`
- `before_update`
- `after_update`
- `before_delete`
- `after_delete`

## Best Practices

### 1. Keep Models Simple

Models should represent database tables with minimal business logic. Complex logic should be in the service layer.

### 2. Use Type Annotations

Always use type annotations with `Mapped[]` for clarity and better type checking:

```python
# Good
class User(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    
# Avoid
class User(Base):
    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String(100))
```

### 3. Define Constraints at the Database Level

Use SQLAlchemy to define constraints enforced by the database:

```python
class User(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(
        String(255), 
        unique=True,  # Enforce uniqueness
        nullable=False  # Require this field
    )
    age: Mapped[int] = mapped_column(CheckConstraint("age >= 18"))  # Check constraint
```

### 4. Use Meaningful Default Values

Provide default values where appropriate:

```python
class Item(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(default=True)  # Default to active
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow  # Automatic timestamp
    )
```

### 5. Add Proper Documentation

Document your models with docstrings explaining purpose and relationships:

```python
class User(Base):
    """
    User model representing application users.
    
    Relationships:
        - Has many posts (one-to-many)
        - Has one profile (one-to-one)
    """
    id: Mapped[int] = mapped_column(primary_key=True)
    # ...
```

### 6. Use Soft Deletes When Appropriate

Consider using soft deletes for important data:

```python
class User(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    is_deleted: Mapped[bool] = mapped_column(default=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), 
        nullable=True
    )
```

## Examples

### Example: Basic Model with Timestamps

```python
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class Item(Base):
    """Item model representing products or services."""
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), index=True)
    description: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    price: Mapped[float]
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow, 
        onupdate=datetime.utcnow
    )
```

### Example: Model with Relationships

```python
from typing import List, Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base


class User(Base):
    """
    User model representing application users.
    
    Relationships:
        - Has many orders (one-to-many)
    """
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    username: Mapped[str] = mapped_column(String(50), index=True, unique=True)
    email: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    full_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Relationships
    orders: Mapped[List["Order"]] = relationship("Order", back_populates="user")


class Order(Base):
    """
    Order model representing user purchases.
    
    Relationships:
        - Belongs to a user (many-to-one)
        - Has many items (one-to-many)
    """
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    total_amount: Mapped[float]
    
    # Foreign keys
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    
    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="orders")
    items: Mapped[List["OrderItem"]] = relationship("OrderItem", back_populates="order")


class OrderItem(Base):
    """
    OrderItem model representing items within an order.
    
    Relationships:
        - Belongs to an order (many-to-one)
    """
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    quantity: Mapped[int]
    unit_price: Mapped[float]
    
    # Foreign keys
    order_id: Mapped[int] = mapped_column(ForeignKey("order.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("product.id"))
    
    # Relationships
    order: Mapped["Order"] = relationship("Order", back_populates="items")
    product: Mapped["Product"] = relationship("Product")
```

### Example: Model with Enums and Custom Types

```python
import enum
from datetime import date
from typing import Optional

from sqlalchemy import Enum, Date, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class SubscriptionType(enum.Enum):
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


class Subscription(Base):
    """Subscription model for user service plans."""
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    
    # Using enum type
    type: Mapped[SubscriptionType] = mapped_column(
        Enum(SubscriptionType), default=SubscriptionType.FREE
    )
    
    # Date fields
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    
    # Billing information
    billing_address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    payment_method: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
``` 