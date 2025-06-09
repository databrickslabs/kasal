# Kasal Authorization Model

## Overview

Kasal is designed as a Databricks App that leverages Databricks OAuth for authentication. The platform uses group-based access control to provide multi-tenant data isolation within the Databricks workspace.

## Authentication

Authentication is handled entirely by Databricks OAuth. When deployed as a Databricks App:

- Users authenticate through their Databricks workspace login
- User context is automatically extracted from Databricks headers
- No separate login system is required

## Group-Based Access Control

Kasal uses Databricks groups for data isolation and access control:

```typescript
interface Group {
  id: string;              // Group UUID
  name: string;            // Group name from Databricks
  created_at: string;      // Creation timestamp
  updated_at: string;      // Last update timestamp
}
```

## Data Isolation

All data in Kasal is scoped to groups for multi-tenant isolation:

- Agents, crews, tasks, and executions are associated with specific groups
- Users can only access data within their assigned groups
- API endpoints automatically filter data by group context

## Permission Model

Access control is simplified through group membership:

- All authenticated Databricks users can access Kasal
- Data access is controlled through group-based filtering
- No complex role-based permissions required

## Implementation

### User Context Middleware

Kasal extracts user context from Databricks headers:

```python
async def user_context_middleware(request: Request, call_next):
    """Extract user context from Databricks headers"""
    # Extract user email and group information from Databricks headers
    user_email = request.headers.get("X-Forwarded-User")
    user_groups = request.headers.get("X-Forwarded-Groups", "").split(",")
    
    # Set context for request processing
    request.state.user_email = user_email
    request.state.user_groups = user_groups
    
    response = await call_next(request)
    return response
```

### Group Context Dependency

API endpoints use group context for data filtering:

```python
async def get_group_context(request: Request) -> Dict[str, Any]:
    """Get group context from request"""
    user_email = getattr(request.state, "user_email", None)
    user_groups = getattr(request.state, "user_groups", [])
    
    return {
        "user_email": user_email,
        "group_ids": user_groups
    }
```

### Database Models

```python
class Group(Base):
    __tablename__ = "groups"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# All AI entities include group_id for isolation
class Agent(Base):
    # ... other fields
    group_id = Column(String, ForeignKey("groups.id"), nullable=False)

class Crew(Base):
    # ... other fields  
    group_id = Column(String, ForeignKey("groups.id"), nullable=False)

class Task(Base):
    # ... other fields
    group_id = Column(String, ForeignKey("groups.id"), nullable=False)
``` 