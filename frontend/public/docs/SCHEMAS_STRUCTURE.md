# Schemas Folder Structure

This document provides a detailed guide to the organization and structure of the schemas folder in our FastAPI backend.

## Overview

The `src/schemas/` directory contains all Pydantic models used for data validation, serialization, and documentation. These schemas are organized by domain and follow a consistent pattern to ensure maintainability and scalability.

## Directory Structure

```
src/schemas/
├── __init__.py                 # Re-exports important schemas for easier imports
├── base.py                     # Common base schemas and mixins
├── item/                       # Item domain schemas
│   ├── __init__.py             # Re-exports from this domain
│   ├── item.py                 # Main item schemas
│   └── item_category.py        # Related item category schemas
├── user/                       # User domain schemas
│   ├── __init__.py             # Re-exports from this domain
│   ├── user.py                 # Main user schemas
│   ├── profile.py              # User profile schemas
│   └── auth.py                 # Authentication-related schemas
├── order/                      # Order domain schemas
│   ├── __init__.py             # Re-exports from this domain
│   ├── order.py                # Main order schemas
│   └── order_item.py           # Order item schemas
└── common/                     # Shared/common schemas
    ├── __init__.py             # Re-exports common schemas
    ├── pagination.py           # Pagination schemas
    ├── responses.py            # Common response schemas
    └── filters.py              # Query filter schemas
```

## Organization Principles

### Domain-Based Organization

Schemas are organized by domain (business entity) to maintain separation of concerns:

- Each major domain has its own subdirectory (`item/`, `user/`, `order/`, etc.)
- Related schemas within a domain are grouped in the same file or closely related files
- Common/shared schemas are kept in the `common/` directory

### Re-export Pattern

To make imports cleaner throughout the application, we use the re-export pattern:

```python
# src/schemas/__init__.py
from src.schemas.item import Item, ItemCreate, ItemUpdate
from src.schemas.user import User, UserCreate, UserUpdate
from src.schemas.common import PaginatedResponse, ErrorResponse

# This allows imports like:
# from src.schemas import User, ItemCreate
# Instead of:
# from src.schemas.user import User
# from src.schemas.item import ItemCreate
```

### File Naming Conventions

- Use singular nouns for schema files (`item.py`, not `items.py`)
- Use snake_case for filenames
- Name files after the primary entity they represent
- Use descriptive suffixes for related schemas (e.g., `item_category.py`)

## Schema Organization Within Files

Each schema file follows a consistent organization pattern:

```python
# Imports
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

# Base schemas
class BaseItem(BaseModel):
    """Base schema with common item attributes."""
    name: str
    description: Optional[str] = None
    
# Input schemas
class ItemCreate(BaseItem):
    """Schema for creating a new item."""
    category_id: int

class ItemUpdate(BaseModel):
    """Schema for updating an existing item."""
    name: Optional[str] = None
    description: Optional[str] = None
    category_id: Optional[int] = None
    
# Response schemas
class Item(BaseItem):
    """Schema for item responses."""
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
```

## Common Schema Types in Each Domain

Each domain typically includes these schema types:

1. **Base Schemas**: Common attributes shared across schemas (e.g., `BaseItem`)
2. **Create Schemas**: Validation for resource creation (e.g., `ItemCreate`)
3. **Update Schemas**: Validation for resource updates (e.g., `ItemUpdate`)
4. **Response Schemas**: Shapes of API responses (e.g., `Item`)
5. **Query Schemas**: Parameters for filtering/querying (e.g., `ItemQuery`)

## Common Folder

The `common/` folder contains schemas that are used across multiple domains:

- **Pagination**: Schemas for paginated responses
- **Responses**: Standard response wrappers (success, error)
- **Filters**: Common query parameter schemas

Example:
```python
# src/schemas/common/pagination.py
from typing import Generic, List, TypeVar
from pydantic import BaseModel

T = TypeVar('T')

class PaginationParams(BaseModel):
    """Query parameters for pagination."""
    page: int = 1
    limit: int = 10

class PaginatedResponse(BaseModel, Generic[T]):
    """A paginated response containing items of type T."""
    items: List[T]
    total: int
    page: int
    limit: int
    pages: int
```

## Best Practices for Schema Organization

1. **Keep related schemas together**: Group schemas that are commonly used together
2. **Don't over-nest**: Avoid creating deep directory hierarchies
3. **Be consistent with naming**: Follow established naming conventions
4. **Provide re-exports**: Make imports easier with proper re-exports
5. **Document relationships**: Comment on how schemas relate to each other
6. **Avoid circular imports**: Structure files to prevent circular dependencies

## Integration with API Routes

The schema folder structure should mirror the API route structure where possible:

- For `/api/v1/items` endpoints, use schemas from `schemas/item/`
- For `/api/v1/users` endpoints, use schemas from `schemas/user/`

This makes it easier to locate the relevant schemas for each endpoint.

## Schema Evolution and Versioning

When APIs evolve, schemas may need versioning:

- Keep backwards-compatible changes in existing schemas when possible
- For breaking changes, consider creating versioned schemas (e.g., `item_v2.py`)
- Document deprecated schemas and fields with appropriate warnings

## Conclusion

Following these organization principles makes our schema structure maintainable, scalable, and easy to navigate. The domain-driven organization ensures that related schemas are grouped together logically, while the consistent file and schema naming makes the codebase more predictable and easier to work with. 