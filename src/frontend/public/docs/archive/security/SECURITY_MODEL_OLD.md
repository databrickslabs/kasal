# Kasal Security Model

## Overview

Kasal implements a multi-tenant security model with simplified Role-Based Access Control (RBAC) designed for AI agent workflow orchestration. The system provides data isolation between users and groups while enabling secure collaboration on AI workflows.

## Table of Contents

- [Core Security Principles](#core-security-principles)
- [Group-Based Architecture](#group-based-architecture)
- [Simplified Role-Based Access Control](#simplified-role-based-access-control)
- [Data Isolation](#data-isolation)
- [Group Management](#group-management)
- [Authentication & Authorization](#authentication--authorization)
- [Security Guarantees](#security-guarantees)
- [Threat Model](#threat-model)
- [Implementation Details](#implementation-details)

## Core Security Principles

### 1. **Privacy by Default**
- Users without group assignments get complete data isolation
- Individual group IDs ensure no cross-user data access
- Zero trust approach - access must be explicitly granted

### 2. **Explicit Group Membership**
- Group access requires admin-managed assignments
- No automatic group enrollment based on email domains
- Clear audit trail of all group assignments

### 3. **Simplified Access Control**
- Three-tier role system (Admin, Editor, Operator)
- Role-based permissions within groups
- Clear separation of concerns with decorator-based enforcement

### 4. **Defense in Depth**
- Multiple layers of security controls
- Database-level group isolation
- Application-level access controls with decorator-based permissions
- API-level authentication and authorization

## Group-Based Architecture

### Individual Group Mode

**When Applied:**
- User is not assigned to any groups
- Group lookup fails or returns empty results
- Fallback security mode for maximum isolation

**Security Characteristics:**
```
User: alice@company.com
Group ID: user_alice_company_com
Access Scope: Only Alice's data
Role: Full admin rights (within own group)
```

**Data Isolation:**
- Execution history: Only Alice's workflows
- Agents & Tasks: Only Alice's creations
- Crews: Only Alice's team configurations
- Logs: Only Alice's execution logs

### Multi-Group Mode

**When Applied:**
- User is assigned to one or more groups
- Admin has explicitly granted group membership
- User belongs to active groups

**Security Characteristics:**
```
User: bob@company.com
Groups: ["dev_team", "qa_team"]
Group IDs: ["dev_team", "qa_team"]
Roles: {"dev_team": "admin", "qa_team": "operator"}
Access Scope: Data from both teams (role-dependent)
```

**Data Isolation:**
- Execution history: From all assigned groups
- Agents & Tasks: Shared team resources (role-dependent access)
- Crews: Team configurations (role-dependent modification)
- Logs: Team execution logs (role-dependent visibility)

## Simplified Role-Based Access Control

### Three-Tier Role System

The system implements a simplified three-tier role hierarchy:

#### 1. **Admin** - Full administrative control
- **Group Management:**
  - Create, update, and delete groups
  - Manage user assignments and roles
  - Full administrative oversight
- **Workflow Operations:**
  - All workflow creation, editing, and execution rights
  - System configuration and management
  - Access to all group data and audit logs

#### 2. **Editor** - Workflow development
- **Workflow Development:**
  - Create, modify, and manage agents, tasks, and crews
  - Full workflow creation and editing capabilities
  - Execute workflows and monitor results
- **Resource Management:**
  - Create and manage team resources
  - Share and collaborate on workflow components
  - View execution history and logs

#### 3. **Operator** - Execution access
- **Workflow Execution:**
  - Execute existing workflows and monitor results
  - View execution history and logs
  - Access to shared team resources (read-only)
- **Limited Modification:**
  - Basic workflow parameter adjustments
  - Personal workspace management
  - View team configurations

### Permission Matrix

| Resource | Operator | Editor | Admin |
|----------|----------|--------|-------|
| **Group Management** |
| View group info | ✓ | ✓ | ✓ |
| Manage users | ✗ | ✗ | ✓ |
| Manage roles | ✗ | ✗ | ✓ |
| Delete group | ✗ | ✗ | ✓ |
| **Workflows** |
| View workflows | ✓ | ✓ | ✓ |
| Create/Edit workflows | ✗ | ✓ | ✓ |
| Execute workflows | ✓ | ✓ | ✓ |
| Delete workflows | ✗ | ✓ | ✓ |
| **Resources (Agents/Tasks/Crews)** |
| View resources | ✓ | ✓ | ✓ |
| Create resources | ✗ | ✓ | ✓ |
| Edit resources | ✗ | ✓ | ✓ |
| Delete resources | ✗ | ✓ | ✓ |
| **Execution & Logs** |
| View execution history | ✓ | ✓ | ✓ |
| View execution logs | ✓ | ✓ | ✓ |
| Stop executions | Own only | ✓ | ✓ |
| Export logs | ✗ | ✓ | ✓ |
| **Tools & Templates** |
| View tools/templates | ✓ | ✓ | ✓ |
| Create tools/templates | ✗ | ✓ | ✓ |
| Edit tools/templates | ✗ | ✓ | ✓ |
| Delete tools/templates | ✗ | ✓ | ✓ |

### Permission Implementation

#### Decorator-Based Protection
```python
from src.core.permissions import require_roles

@router.delete("/groups/{group_id}")
@require_roles(["admin"])
async def delete_group(group_id: str, group_context: GroupContextDep):
    """Delete group - requires admin role."""
    await group_service.delete_group(group_id)

@router.post("/agents")
@require_roles(["editor", "admin"])
async def create_agent(agent_data: AgentCreateRequest, group_context: GroupContextDep):
    """Create agent - requires editor or admin role."""
    return await agent_service.create_agent(agent_data, group_context)

@router.get("/executions")
@require_roles(["operator", "editor", "admin"])
async def get_executions(group_context: GroupContextDep):
    """Get executions - all roles can view."""
    return await execution_service.get_executions(group_context)
```

#### Role Checking Function
```python
def check_role_in_context(group_context: GroupContext, required_roles: List[str]) -> bool:
    """Check if user has one of the required roles in current group context."""
    user_role = group_context.user_role
    highest_role = getattr(group_context, 'highest_role', None)

    # Check current group role or highest role across all groups
    effective_role = highest_role if highest_role else user_role

    return effective_role and effective_role.lower() in [role.lower() for role in required_roles]
```

## Data Isolation

### Database-Level Isolation

All major data entities include group isolation with automatic filtering:

```sql
-- Example: Execution History Table
CREATE TABLE execution_history (
    id SERIAL PRIMARY KEY,
    group_id VARCHAR(100) NOT NULL,  -- Group isolation key
    created_by_email VARCHAR(255),   -- Audit trail
    job_id UUID,
    status VARCHAR(50),
    created_at TIMESTAMP,
    -- ... other fields
    INDEX idx_group_id (group_id),           -- Performance optimization
    INDEX idx_created_by_email (created_by_email)  -- Audit queries
);
```

**Comprehensive Isolated Entities:**

#### Core Workflow Components
- `execution_history` - All workflow execution records with group isolation
- `execution_logs` - Detailed execution logs filtered by group
- `flow_executions` - Flow execution instances with group boundaries
- `agents` - AI agent definitions scoped to group
- `tasks` - Task configurations with group access control
- `crews` - Team configurations isolated by group
- `flows` - Workflow definitions with group ownership
- `schedules` - Scheduled job executions with group filtering

#### Configuration & Templates
- `templates` - Reusable prompt templates scoped to group
- `tools` - Custom tool definitions with group access
- `model_configs` - LLM model configurations per group
- `engine_configs` - Execution engine settings by group

#### Logging & Monitoring
- `llmlog` - LLM API interaction logs with group isolation
- `execution_trace` - Detailed execution traces filtered by group
- `api_logs` - API access logs with group context

#### User & Group Management
- `groups` - Group definitions and metadata
- `group_users` - User-to-group assignments with roles
- `users` - User profiles with group associations

### Query-Level Filtering

All data queries automatically include group filtering:

```python
# Individual user - single group
WHERE group_id = 'user_alice_company_com'

# Multi-group user - multiple groups
WHERE group_id IN ('dev_team', 'qa_team')
```

### Multi-Layer Isolation Architecture

#### 1. **Request-Level Isolation**
Every HTTP request includes group context extraction:

```python
@dataclass
class GroupContext:
    group_ids: List[str]            # All accessible group IDs
    group_email: str                # User's email for audit
    email_domain: str               # Email domain
    user_id: Optional[str]          # User identifier
    access_token: Optional[str]     # Authentication token
    user_role: Optional[str]        # Role in current/primary group
    highest_role: Optional[str]     # Highest role across all groups

    @property
    def primary_group_id(self) -> str:
        """Primary group for creating new data."""
        return self.group_ids[0] if self.group_ids else None
```

#### 2. **API Endpoint Isolation**
All API endpoints automatically enforce group boundaries:

```python
@router.get("/executions/")
@require_roles(["operator", "editor", "admin"])
async def get_executions(
    group_context: GroupContextDep,  # Automatic group injection
    page: int = 1,
    limit: int = 20
):
    # Service automatically filters by group_context.group_ids
    return await execution_service.get_executions_paginated(
        group_context=group_context,
        page=page,
        limit=limit
    )
```

#### 3. **Service-Level Isolation**
All business logic services respect group boundaries:

```python
class ExecutionHistoryService:
    async def get_executions_paginated(
        self,
        group_context: GroupContext,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get executions with automatic group filtering."""

        # Automatic group ID filtering
        return await self.repository.get_paginated_by_group(
            group_ids=group_context.group_ids,
            page=page,
            limit=limit
        )
```

#### 4. **Repository-Level Isolation**
Data access layer enforces group filtering at query level:

```python
class ExecutionHistoryRepository:
    async def get_paginated_by_group(
        self,
        group_ids: List[str],
        page: int,
        limit: int
    ) -> Dict[str, Any]:
        """Repository ensures group filtering in all queries."""

        # Base query with group filtering
        base_query = select(ExecutionHistory).where(
            ExecutionHistory.group_id.in_(group_ids)
        )

        # Count query with same group filtering
        count_query = select(func.count(ExecutionHistory.id)).where(
            ExecutionHistory.group_id.in_(group_ids)
        )

        # Both queries automatically respect group boundaries
```

## Individual vs Group Permissions

### Individual Mode
- User has full admin rights over their private group
- No collaboration features available
- Complete isolation from other users
- All operations available within personal scope

### Group Mode
- Permissions determined by role within each group
- Can have different roles in different groups
- Access to shared group resources
- Role-based restrictions apply

## Group Management

### Group Creation

**Who Can Create:**
- System administrators
- Users with admin privileges

**Process:**
1. Admin creates group with unique identifier
2. Group gets assigned domain/namespace
3. Initial admin user assigned
4. Group becomes available for user assignment

### User Assignment

**Security Requirements:**
- Must be performed by group admin
- Explicit role assignment required
- Audit trail maintained for all changes

**Assignment Process:**
```python
# Secure user assignment
group_service.assign_user_to_group(
    group_id="dev_team",
    user_email="alice@company.com",
    role="editor",
    assigned_by_email="admin@company.com"
)
```

### Group Deletion

**Security Safeguards:**
- Only group admins can delete groups
- Confirmation required for destructive action
- All associated data permanently removed
- Users automatically moved to individual mode

## Authentication & Authorization

### Request Flow

1. **Authentication:** User identity verified via headers/tokens
2. **Group Resolution:** System determines user's group context
3. **Authorization:** Request permissions validated against group access
4. **Data Filtering:** Results filtered to accessible groups only

### Headers & Context

**Databricks Apps Integration:**
```http
X-Forwarded-Email: alice@company.com
X-Forwarded-Access-Token: dapi1234567890abcdef
```

**Development Mode:**
```python
# Development override for testing
MOCK_USER_EMAIL = "alice@company.com"
```

### Group Context Resolution

```python
async def extract_group_context(request: Request) -> GroupContext:
    email = request.headers.get('X-Forwarded-Email')

    # Look up user's group memberships
    user_groups = await get_user_group_memberships(email)

    if not user_groups:
        # Individual mode - private group
        group_ids = [generate_individual_group_id(email)]
        user_role = "admin"  # Full rights in individual mode
    else:
        # Group mode - shared groups
        group_ids = [group.id for group in user_groups]
        user_role = user_groups[0].role  # Primary group role
        highest_role = max(group.role for group in user_groups)

    return GroupContext(
        group_ids=group_ids,
        group_email=email,
        user_role=user_role,
        highest_role=highest_role,
        # ... other fields
    )
```

## Security Guarantees

### Data Isolation Guarantees

1. **Individual Users:** Cannot access other users' data
2. **Group Members:** Can only access assigned group data
3. **Cross-Group:** Users in multiple groups see combined data
4. **Admin Separation:** Group admins cannot access other groups

### Audit & Compliance

- **Creation Tracking:** All data includes creator information
- **Access Logging:** All data access logged with user context
- **Change Audit:** Group membership changes tracked
- **Deletion Logs:** Permanent deletion events recorded

### Data Retention

- **Individual Data:** Persists until user account deletion
- **Group Data:** Persists until group deletion
- **Audit Logs:** Retained according to compliance requirements
- **Backup Security:** Encrypted backups maintain tenant isolation

## Threat Model

### Threats Mitigated

#### 1. **Unauthorized Data Access**
- **Threat:** User accessing another user's private data
- **Mitigation:** Individual tenant isolation with unique tenant IDs
- **Detection:** Query-level filtering prevents cross-tenant access

#### 2. **Group Data Leakage**
- **Threat:** User accessing groups they don't belong to
- **Mitigation:** Explicit group membership validation
- **Detection:** All queries filtered by user's assigned tenant IDs

#### 3. **Role Elevation**
- **Threat:** User gaining unauthorized permissions within group
- **Mitigation:** Decorator-based role enforcement with explicit assignments
- **Detection:** Role checks on every sensitive operation

#### 4. **Admin Overreach**
- **Threat:** Group admin accessing other groups or individual data
- **Mitigation:** Admin permissions scoped to assigned groups only
- **Detection:** Audit logging of all admin actions

### Residual Risks

#### 1. **Platform Admin Access**
- **Risk:** Platform administrators have database access
- **Mitigation:** Limited to necessary personnel, audit logging
- **Monitoring:** Database access logging and review

#### 2. **Application Vulnerabilities**
- **Risk:** Security bugs could bypass tenant filtering
- **Mitigation:** Code review, security testing, tenant validation
- **Monitoring:** Automated security scanning, penetration testing

#### 3. **Backup/Export Data**
- **Risk:** Data exports could contain cross-tenant information
- **Mitigation:** Export operations respect tenant boundaries
- **Monitoring:** Export audit logging and validation

## Implementation Details

### Group ID Generation

#### Individual Groups
```python
def generate_individual_group_id(email: str) -> str:
    """Generate unique group ID for individual user."""
    sanitized = email.replace("@", "_").replace(".", "_")
    return f"user_{sanitized}".lower()

# Examples:
# alice@company.com → user_alice_company_com
# bob.smith@startup.io → user_bob_smith_startup_io
```

#### Shared Groups
```python
def generate_group_id(domain: str) -> str:
    """Generate group ID for shared group."""
    return domain.replace(".", "_").replace("-", "_").lower()

# Examples:
# dev-team → dev_team
# marketing.team → marketing_team
```

### Database Schema Security

#### Group Columns
All major tables include:
```sql
group_id VARCHAR(100) NOT NULL,     -- Primary isolation key
group_email VARCHAR(255),           -- Audit trail
INDEX idx_group_id (group_id),     -- Query performance
INDEX idx_group_email (group_email) -- Audit queries
```

#### Foreign Key Constraints
```sql
-- Group membership table
CREATE TABLE group_users (
    id VARCHAR(100) PRIMARY KEY,
    group_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'operator',
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_group (user_id, group_id)
);
```

### API Security Implementation

#### Group Context Dependency
```python
async def get_group_context(request: Request) -> GroupContext:
    """Extract and validate group context from request."""
    context = await extract_group_context_from_request(request)
    if not context or not context.is_valid():
        raise HTTPException(401, "Invalid group context")
    return context

# Usage in endpoints
@router.get("/executions/")
async def get_executions(
    group_context: Annotated[GroupContext, Depends(get_group_context)]
):
    return await execution_service.get_executions(
        group_ids=group_context.group_ids
    )
```

#### Query Filtering
```python
async def get_executions(self, group_ids: List[str]) -> List[Execution]:
    """Get executions with automatic group filtering."""
    stmt = select(ExecutionHistory).where(
        ExecutionHistory.group_id.in_(group_ids)
    ).order_by(ExecutionHistory.created_at.desc())

    result = await session.execute(stmt)
    return result.scalars().all()
```

---

## Summary

The Kasal security model provides robust multi-group isolation with simplified role-based access control. The three-tier role system (Admin, Editor, Operator) provides clear permission boundaries while maintaining simplicity. By automatically adapting between individual and group modes, it ensures both privacy and productivity while maintaining strong security boundaries.

For technical implementation details, see the source code in:
- `/src/utils/user_context.py` - Group context management
- `/src/core/dependencies.py` - Security dependencies
- `/src/core/permissions.py` - Role-based permission enforcement
- `/src/services/group_service.py` - Group management
- `/src/api/group_router.py` - Admin API endpoints