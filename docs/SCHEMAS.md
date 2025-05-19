# Pydantic Schemas Guide

This document provides comprehensive documentation for Pydantic schemas in our backend architecture.

## Table of Contents

- [Overview](#overview)
- [Structure and Conventions](#structure-and-conventions)
- [Schema Types](#schema-types)
- [Field Validation](#field-validation)
- [Schema Composition](#schema-composition)
- [Model Configuration](#model-configuration)
- [Integration with SQLAlchemy](#integration-with-sqlalchemy)
- [Data Conversion](#data-conversion)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Overview

Pydantic schemas are Python classes that provide data validation, serialization, and documentation. In our architecture, they serve several critical purposes:

- **API Input Validation**: Validate incoming request data
- **Response Serialization**: Format outgoing response data
- **Type Checking**: Ensure type safety at runtime
- **Documentation**: Generate OpenAPI docs for the API
- **Data Transformation**: Convert between API and database formats

Schemas are stored in the `src/schemas/` directory, with schemas for each domain typically in their own file.

## Structure and Conventions

### File Organization

```
src/schemas/
├── __init__.py         # Exposes schemas for easier imports
├── item.py             # Schemas related to items
├── user.py             # Schemas related to users
└── other_domain.py     # Other domain-specific schemas
```

### Naming Conventions

We follow consistent naming patterns for different schema types:

- Base schemas: `Base{Resource}` (e.g., `BaseItem`)
- Creation schemas: `{Resource}Create` (e.g., `ItemCreate`)
- Update schemas: `{Resource}Update` (e.g., `ItemUpdate`)
- Response schemas: `{Resource}` (e.g., `Item`)
- Internal schemas: `{Resource}InDB` (e.g., `ItemInDB`)

## Schema Types

In our architecture, we use different schema types for different purposes:

### Base Schemas

Base schemas define common attributes shared by multiple schemas:

```python
class BaseItem(BaseModel):
    """Base schema with common item attributes."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    price: float = Field(..., gt=0)
    is_active: bool = True
```

### Create Schemas

Used to validate data for creating new resources:

```python
class ItemCreate(BaseItem):
    """Schema for creating a new item."""
    # Inherits all fields from BaseItem
    # May add additional fields specific to creation
    category_id: int
```

### Update Schemas

Used to validate data for updating existing resources, usually with all fields optional:

```python
class ItemUpdate(BaseModel):
    """Schema for updating an existing item."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    price: Optional[float] = Field(None, gt=0)
    is_active: Optional[bool] = None
    category_id: Optional[int] = None
```

### Response Schemas

Used for API responses, including all fields that should be returned to clients:

```python
class Item(BaseItem):
    """Schema for item responses."""
    id: int
    created_at: datetime
    updated_at: datetime
    category: Optional["Category"] = None
    
    class Config:
        from_attributes = True
```

### Internal Schemas

Used for internal data processing, may include fields not exposed to API:

```python
class ItemInDB(Item):
    """Schema for internal use with additional fields."""
    deleted: bool = False
    version: int = 1
    internal_notes: Optional[str] = None
```

## Field Validation

Pydantic provides powerful field validation capabilities:

### Basic Validators

```python
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, regex="^[a-zA-Z0-9_-]+$")
    email: EmailStr
    password: str = Field(..., min_length=8)
    age: int = Field(..., ge=18)  # Must be greater than or equal to 18
    website: Optional[HttpUrl] = None
```

### Custom Validators

Use `@field_validator` for custom field validations:

```python
from pydantic import BaseModel, field_validator

class UserCreate(BaseModel):
    username: str
    password: str
    password_confirm: str
    
    @field_validator("username")
    @classmethod
    def username_alphanumeric(cls, v):
        if not v.isalnum():
            raise ValueError("Username must be alphanumeric")
        return v
    
    @field_validator("password_confirm")
    @classmethod
    def passwords_match(cls, v, info):
        if "password" in info.data and v != info.data["password"]:
            raise ValueError("Passwords do not match")
        return v
```

### Model Validators

Use `@model_validator` for validations across multiple fields:

```python
from pydantic import BaseModel, model_validator

class Order(BaseModel):
    items: List[OrderItem]
    discount: float = 0.0
    tax_rate: float
    
    @model_validator(mode='after')
    def check_total_with_discount(self):
        total = sum(item.price * item.quantity for item in self.items)
        if self.discount > total:
            raise ValueError("Discount cannot be greater than total")
        return self
```

## Schema Composition

Schemas can be composed and nested to represent complex structures:

### Nested Schemas

```python
class Address(BaseModel):
    street: str
    city: str
    state: str
    zip_code: str
    country: str

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    address: Address  # Nested schema
```

### List Fields

```python
class TagCreate(BaseModel):
    name: str

class PostCreate(BaseModel):
    title: str
    content: str
    tags: List[TagCreate]  # List of nested schemas
```

### Union Types

```python
class TextContent(BaseModel):
    type: Literal["text"]
    text: str

class ImageContent(BaseModel):
    type: Literal["image"]
    url: HttpUrl
    caption: Optional[str] = None

class PostContent(BaseModel):
    title: str
    content: Union[TextContent, ImageContent]  # Can be either type
```

## Model Configuration

Customize Pydantic behavior with model configuration:

```python
class User(BaseModel):
    id: int
    username: str
    email: str
    created_at: datetime
    
    class Config:
        # Allow conversion from ORM models
        from_attributes = True
        
        # Use ISO format for dates in JSON
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }
        
        # Validate assignment to attributes
        validate_assignment = True
        
        # Allow "extra" fields that aren't specified
        extra = "ignore"
```

## Integration with SQLAlchemy

Pydantic integrates with SQLAlchemy models in our architecture:

### Converting SQLAlchemy Models to Pydantic Schema

```python
# With SQLAlchemy model instance "db_item"
item_schema = Item.model_validate(db_item)
```

### Configuration for SQLAlchemy Integration

```python
class Item(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    price: float
    
    class Config:
        from_attributes = True  # Enable ORM mode for SQLAlchemy conversion
```

## Data Conversion

Schemas provide methods for data conversion:

### To Dictionary

```python
item = Item(id=1, name="Example", price=9.99)
item_dict = item.model_dump()  # {"id": 1, "name": "Example", "price": 9.99}
```

### To JSON

```python
item_json = item.model_dump_json()  # '{"id": 1, "name": "Example", "price": 9.99}'
```

### Include/Exclude Fields

```python
# Only include certain fields
item_dict = item.model_dump(include={"name", "price"})

# Exclude certain fields
item_dict = item.model_dump(exclude={"description"})
```

### Nested Include/Exclude

```python
user_with_items = User(
    id=1, 
    username="user1", 
    items=[Item(id=1, name="Item 1"), Item(id=2, name="Item 2")]
)

# Include nested fields
user_dict = user_with_items.model_dump(include={
    "id": True,
    "username": True,
    "items": {"id": True, "name": True}
})
```

## Best Practices

### 1. Define Clear Schema Types

Create distinct schemas for different operations:

```python
# Good: Clear separation of concerns
class ItemCreate(BaseModel):
    # Fields for creating an item
    
class ItemUpdate(BaseModel):
    # Fields for updating an item
    
class Item(BaseModel):
    # Fields for returning an item in responses

# Avoid: One schema for everything
class Item(BaseModel):
    # Mixing create, update, and response fields
```

### 2. Use Field Validation

Always validate input data with appropriate constraints:

```python
# Good: Proper validation
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr  # Validates email format
    age: int = Field(..., ge=18)  # Must be 18 or older
    
# Avoid: Missing validation
class UserCreate(BaseModel):
    username: str
    email: str  # No format validation
    age: int  # No range check
```

### 3. Make Update Schemas Optional

Use `Optional` for all fields in update schemas:

```python
# Good: All fields optional for partial updates
class ItemUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    
# Avoid: Required fields in update schema
class ItemUpdate(BaseModel):
    name: str  # Required field prevents partial updates
    description: Optional[str] = None
    price: float  # Required field prevents partial updates
```

### 4. Use Inheritance for Common Fields

Inherit from base schemas to avoid duplication:

```python
# Good: Using inheritance
class BaseItem(BaseModel):
    name: str
    description: Optional[str] = None
    price: float

class ItemCreate(BaseItem):
    category_id: int
    
class Item(BaseItem):
    id: int
    created_at: datetime
    
# Avoid: Duplicating fields
class ItemCreate(BaseModel):
    name: str  # Duplicated
    description: Optional[str] = None  # Duplicated
    price: float  # Duplicated
    category_id: int
    
class Item(BaseModel):
    id: int
    name: str  # Duplicated
    description: Optional[str] = None  # Duplicated
    price: float  # Duplicated
    created_at: datetime
```

### 5. Document Your Schemas

Add docstrings and field descriptions:

```python
class UserCreate(BaseModel):
    """
    Schema for creating a new user.
    
    Attributes:
        username: User's unique handle, alphanumeric only
        email: User's email address, must be valid format
        password: User's password, minimum 8 characters
    """
    username: str = Field(
        ..., 
        min_length=3, 
        max_length=50, 
        description="User's unique handle, alphanumeric only"
    )
    email: EmailStr = Field(..., description="User's email address")
    password: str = Field(
        ..., 
        min_length=8, 
        description="User's password, minimum 8 characters"
    )
```

### 6. Use Strict Type Validation

Enable strict type validation to catch type errors early:

```python
class Config:
    # Configuration for all schemas
    model_config = {
        "strict": True  # Enforce strict type checking
    }
```

### 7. Keep Presentation Logic Out of Schemas

Avoid adding business logic to schemas - keep them focused on data validation:

```python
# Good: Schema focused on validation
class OrderCreate(BaseModel):
    items: List[OrderItemCreate]
    shipping_address_id: int
    
# Avoid: Adding business logic in schemas
class OrderCreate(BaseModel):
    items: List[OrderItemCreate]
    shipping_address_id: int
    
    def calculate_total(self):
        # Business logic should be in service layer, not in schema
        return sum(item.price * item.quantity for item in self.items)
```

## Examples

### Example: Basic Schema Hierarchy

```python
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field, HttpUrl


class BaseUser(BaseModel):
    """Base user data shared across schemas."""
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_active: bool = True


class UserCreate(BaseUser):
    """Schema for creating a new user."""
    password: str = Field(..., min_length=8)
    password_confirm: str = Field(..., min_length=8)
    
    @field_validator("password_confirm")
    @classmethod
    def passwords_match(cls, v, info):
        if "password" in info.data and v != info.data["password"]:
            raise ValueError("Passwords do not match")
        return v


class UserUpdate(BaseModel):
    """Schema for updating an existing user."""
    email: Optional[EmailStr] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=8)


class User(BaseUser):
    """Schema for user responses."""
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
```

### Example: Nested Schema with Relationships

```python
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class CategoryBase(BaseModel):
    """Base category data."""
    name: str = Field(..., min_length=1, max_length=50)
    description: Optional[str] = None


class CategoryCreate(CategoryBase):
    """Schema for creating a new category."""
    pass


class Category(CategoryBase):
    """Schema for category responses."""
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


class ItemBase(BaseModel):
    """Base item data."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    price: float = Field(..., gt=0)
    is_active: bool = True


class ItemCreate(ItemBase):
    """Schema for creating a new item."""
    category_id: int


class Item(ItemBase):
    """Schema for item responses."""
    id: int
    category_id: int
    category: Optional[Category] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class CategoryWithItems(Category):
    """Category with nested items."""
    items: List[Item] = []
```

### Example: Complex Validation and Custom Types

```python
from datetime import date
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, EmailStr, Field, model_validator


class SubscriptionType(str, Enum):
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


class PaymentMethod(BaseModel):
    """Payment method details."""
    type: str = Field(..., regex="^(credit_card|paypal|bank_transfer)$")
    last_four: Optional[str] = Field(None, regex="^[0-9]{4}$")
    expiry_date: Optional[date] = None
    is_default: bool = False


class AddressCreate(BaseModel):
    """Address creation schema."""
    street: str
    city: str
    state: str
    postal_code: str
    country: str = Field(..., min_length=2, max_length=2)  # ISO country code


class SubscriptionCreate(BaseModel):
    """Subscription creation schema."""
    type: SubscriptionType
    payment_method_id: Optional[int] = None
    address_id: Optional[int] = None
    coupon_code: Optional[str] = None
    
    @model_validator(mode='after')
    def check_payment_method_for_paid_plans(self):
        if self.type != SubscriptionType.FREE and not self.payment_method_id:
            raise ValueError(
                "Payment method is required for non-free subscription types"
            )
        return self
```

### Example: Schema with Complex Computed Fields

```python
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, computed_field


class OrderItem(BaseModel):
    """Order item within an order."""
    product_id: int
    product_name: str
    quantity: int = Field(..., gt=0)
    unit_price: float = Field(..., gt=0)
    
    @computed_field
    def subtotal(self) -> float:
        return self.quantity * self.unit_price


class OrderCreate(BaseModel):
    """Order creation schema."""
    user_id: int
    items: List[OrderItem]
    shipping_address_id: int
    billing_address_id: Optional[int] = None
    coupon_code: Optional[str] = None


class Order(BaseModel):
    """Order response schema."""
    id: int
    user_id: int
    items: List[OrderItem]
    shipping_address_id: int
    billing_address_id: Optional[int] = None
    status: str
    created_at: datetime
    
    @computed_field
    def item_count(self) -> int:
        return sum(item.quantity for item in self.items)
    
    @computed_field
    def subtotal(self) -> float:
        return sum(item.subtotal for item in self.items)
    
    @computed_field
    def total(self) -> float:
        # Could include tax, shipping, etc.
        return self.subtotal
    
    class Config:
        from_attributes = True
``` 