# Kasal Security Model

## Overview

Kasal implements a multi-tenant security model with Role-Based Access Control (RBAC) designed for AI agent workflow orchestration. The system provides data isolation between users and groups while enabling secure collaboration on AI workflows.

## Table of Contents

- [Core Security Principles](#core-security-principles)
- [Group-Based Architecture](#group-based-architecture)
- [Role-Based Access Control (RBAC)](#role-based-access-control-rbac)
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

### 3. **Least Privilege Access**
- Role-based permissions within groups (Admin, Manager, User, Viewer)
- Users only see data from their assigned groups
- Granular privilege system for fine-grained control

### 4. **Defense in Depth**
- Multiple layers of security controls
- Database-level group isolation
- Application-level access controls with RBAC
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
Roles: {"dev_team": "admin", "qa_team": "user"}
Access Scope: Data from both teams (role-dependent)
```

**Data Isolation:**
- Execution history: From all assigned groups
- Agents & Tasks: Shared team resources (role-dependent access)
- Crews: Team configurations (role-dependent modification)
- Logs: Team execution logs (role-dependent visibility)

## Role-Based Access Control (RBAC)

### Role Hierarchy

The system implements a four-tier role hierarchy with increasing privileges:

#### 1. **Viewer** - Read-only access
- **Group Privileges:**
  - `group:read` - View group information
- **Workflow Privileges:**
  - `workflow:read` - View workflows and executions
  - `execution_history:read` - View execution history
  - `execution_logs:read` - View execution logs
- **Resource Privileges:**
  - `agent:read`, `task:read`, `crew:read` - View team resources
  - `template:read`, `tool:read` - View shared templates and tools

#### 2. **User** - Standard workflow execution
- **Inherits all Viewer privileges, plus:**
- **Workflow Privileges:**
  - `workflow:create`, `workflow:update`, `workflow:delete` - Manage workflows
  - `workflow:execute` - Execute workflows
  - `execution:create` - Create executions
- **Resource Privileges:**
  - `agent:create`, `agent:update`, `agent:delete` - Manage agents
  - `task:create`, `task:update`, `task:delete` - Manage tasks
  - `crew:create`, `crew:update`, `crew:delete` - Manage crews
  - `template:create`, `template:update` - Create and edit templates
- **Limited Tool Access:**
  - `tool:create`, `tool:update` - Create and modify personal tools

#### 3. **Manager** - Team management capabilities
- **Inherits all User privileges, plus:**
- **User Management:**
  - `group:manage_users` - Add/remove users from group
  - `user:update_role` - Change user roles (up to Manager level)
- **Advanced Resource Management:**
  - `tool:delete` - Delete tools
  - `template:delete` - Delete templates
  - `schedule:create`, `schedule:update`, `schedule:delete` - Manage schedules
- **Team Operations:**
  - `execution:stop`, `execution:restart` - Control team executions
  - `logs:export` - Export team logs

#### 4. **Admin** - Full administrative control
- **Inherits all Manager privileges, plus:**
- **Group Administration:**
  - `group:create`, `group:update`, `group:delete` - Full group management
  - `group:manage_roles` - Assign any role including Admin
- **System Operations:**
  - `user:create`, `user:delete` - Manage user accounts
  - `system:backup`, `system:restore` - System-level operations
- **Security Management:**
  - `audit:read` - Access audit logs
  - `security:manage` - Manage security settings

### Permission Matrix

| Resource | Viewer | User | Manager | Admin |
|----------|--------|------|---------|-------|
| **Group Management** |
| View group info | ✓ | ✓ | ✓ | ✓ |
| Manage users | ✗ | ✗ | ✓ | ✓ |
| Manage roles | ✗ | ✗ | Limited | ✓ |
| Delete group | ✗ | ✗ | ✗ | ✓ |
| **Workflows** |
| View workflows | ✓ | ✓ | ✓ | ✓ |
| Create/Edit workflows | ✗ | ✓ | ✓ | ✓ |
| Execute workflows | ✗ | ✓ | ✓ | ✓ |
| Delete workflows | ✗ | ✓ | ✓ | ✓ |
| **Resources (Agents/Tasks/Crews)** |
| View resources | ✓ | ✓ | ✓ | ✓ |
| Create resources | ✗ | ✓ | ✓ | ✓ |
| Edit own resources | ✗ | ✓ | ✓ | ✓ |
| Edit team resources | ✗ | Limited | ✓ | ✓ |
| Delete resources | ✗ | Own only | ✓ | ✓ |
| **Execution & Logs** |
| View execution history | ✓ | ✓ | ✓ | ✓ |
| View execution logs | ✓ | ✓ | ✓ | ✓ |
| Stop executions | ✗ | Own only | ✓ | ✓ |
| Export logs | ✗ | ✗ | ✓ | ✓ |
| **Tools & Templates** |
| View tools/templates | ✓ | ✓ | ✓ | ✓ |
| Create tools/templates | ✗ | ✓ | ✓ | ✓ |
| Edit tools/templates | ✗ | Own only | ✓ | ✓ |
| Delete tools/templates | ✗ | ✗ | ✓ | ✓ |

### Privilege Implementation

#### Privilege Constants
```python
class Privileges:
    # Group Management
    GROUP_CREATE = "group:create"
    GROUP_READ = "group:read"
    GROUP_UPDATE = "group:update"
    GROUP_DELETE = "group:delete"
    GROUP_MANAGE_USERS = "group:manage_users"
    GROUP_MANAGE_ROLES = "group:manage_roles"
    
    # Workflow Management
    WORKFLOW_CREATE = "workflow:create"
    WORKFLOW_READ = "workflow:read"
    WORKFLOW_UPDATE = "workflow:update"
    WORKFLOW_DELETE = "workflow:delete"
    WORKFLOW_EXECUTE = "workflow:execute"
    
    # Resource Management
    AGENT_CREATE = "agent:create"
    AGENT_READ = "agent:read"
    AGENT_UPDATE = "agent:update"
    AGENT_DELETE = "agent:delete"
    
    # Execution Management
    EXECUTION_CREATE = "execution:create"
    EXECUTION_READ = "execution:read"
    EXECUTION_STOP = "execution:stop"
    EXECUTION_RESTART = "execution:restart"
    
    # And many more...
```

#### Role-Privilege Mapping
```python
ROLE_PRIVILEGES = {
    GroupUserRole.VIEWER: [
        Privileges.GROUP_READ,
        Privileges.WORKFLOW_READ,
        Privileges.AGENT_READ,
        Privileges.TASK_READ,
        Privileges.CREW_READ,
        Privileges.EXECUTION_READ,
        Privileges.EXECUTION_HISTORY_READ,
        Privileges.EXECUTION_LOGS_READ,
        # ... more read-only privileges
    ],
    
    GroupUserRole.USER: [
        # Inherits all Viewer privileges +
        Privileges.WORKFLOW_CREATE,
        Privileges.WORKFLOW_UPDATE,
        Privileges.WORKFLOW_DELETE,
        Privileges.WORKFLOW_EXECUTE,
        Privileges.AGENT_CREATE,
        Privileges.AGENT_UPDATE,
        Privileges.AGENT_DELETE,
        # ... more user privileges
    ],
    
    GroupUserRole.MANAGER: [
        # Inherits all User privileges +
        Privileges.GROUP_MANAGE_USERS,
        Privileges.USER_UPDATE_ROLE,
        Privileges.SCHEDULE_CREATE,
        Privileges.SCHEDULE_UPDATE,
        Privileges.SCHEDULE_DELETE,
        Privileges.EXECUTION_STOP,
        Privileges.EXECUTION_RESTART,
        # ... more manager privileges
    ],
    
    GroupUserRole.ADMIN: [
        # Inherits all Manager privileges +
        Privileges.GROUP_CREATE,
        Privileges.GROUP_UPDATE,
        Privileges.GROUP_DELETE,
        Privileges.GROUP_MANAGE_ROLES,
        Privileges.USER_CREATE,
        Privileges.USER_DELETE,
        Privileges.SYSTEM_BACKUP,
        Privileges.AUDIT_READ,
        # ... all privileges
    ]
}
```

#### Permission Checking
```python
async def check_permission(
    user_id: str, 
    group_id: str, 
    required_privilege: str
) -> bool:
    """Check if user has required privilege in group."""
    
    # Get user's role in the group
    user_role = await get_user_role_in_group(user_id, group_id)
    if not user_role:
        return False
    
    # Get privileges for that role
    role_privileges = ROLE_PRIVILEGES.get(user_role, [])
    
    # Check if privilege is granted
    return required_privilege in role_privileges
```

#### Decorator for Endpoint Protection
```python
def require_privilege(privilege: str):
    """Decorator to protect endpoints with privilege requirements."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Extract group context and user from request
            group_context = kwargs.get('group_context')
            if not group_context:
                raise HTTPException(403, "Group context required")
            
            # Check permission for primary group
            has_permission = await check_permission(
                user_id=group_context.user_id,
                group_id=group_context.primary_group_id,
                required_privilege=privilege
            )
            
            if not has_permission:
                raise HTTPException(403, f"Missing privilege: {privilege}")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# Usage in endpoints
@router.delete("/groups/{group_id}")
@require_privilege(Privileges.GROUP_DELETE)
async def delete_group(group_id: str, group_context: GroupContextDep):
    """Delete group - requires admin privilege."""
    await group_service.delete_group(group_id)
```

## Data Isolation

### Database-Level Isolation

All major data entities include group isolation with automatic filtering at every data access point:

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
- `user_roles` - RBAC role assignments within groups

### Query-Level Filtering

All data queries automatically include group filtering:

```python
# Individual user - single group
WHERE group_id = 'user_alice_company_com'

# Multi-group user - multiple groups
WHERE group_id IN ('dev_team', 'qa_team')
```

### Multi-Layer Isolation Architecture

The system implements isolation at multiple levels to ensure comprehensive data protection:

#### 1. **Request-Level Isolation**
Every HTTP request includes group context extraction with RBAC:

```python
@dataclass
class GroupContext:
    group_ids: List[str]            # All accessible group IDs
    group_email: str                # User's email for audit
    email_domain: str               # Email domain
    user_id: Optional[str]          # User identifier
    access_token: Optional[str]     # Authentication token
    user_roles: Dict[str, str]      # Role per group {"group_id": "role"}

    @property
    def primary_group_id(self) -> str:
        """Primary group for creating new data."""
        return self.group_ids[0] if self.group_ids else None
    
    def get_role_in_group(self, group_id: str) -> Optional[str]:
        """Get user's role in specific group."""
        return self.user_roles.get(group_id)
```

#### 2. **API Endpoint Isolation with RBAC**
All API endpoints automatically enforce group boundaries and role-based permissions:

```python
# Every endpoint includes group context with role-based filtering
@router.get("/executions/")
@require_privilege(Privileges.EXECUTION_READ)
async def get_executions(
    group_context: GroupContextDep,  # Automatic group injection with roles
    page: int = 1,
    limit: int = 20
):
    # Service automatically filters by group_context.group_ids and user role
    return await execution_service.get_executions_paginated(
        group_context=group_context,
        page=page, 
        limit=limit
    )
```

#### 3. **Service-Level Isolation with RBAC**
All business logic services respect group boundaries and role permissions:

```python
class ExecutionHistoryService:
    async def get_executions_paginated(
        self, 
        group_context: GroupContext,
        page: int = 1, 
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get executions with automatic group filtering and role-based access."""
        
        # Check user has read permission for executions
        await self.check_permission(
            group_context, 
            Privileges.EXECUTION_READ
        )
        
        # Automatic group ID filtering with role consideration
        return await self.repository.get_paginated_by_group(
            group_ids=group_context.group_ids,
            user_roles=group_context.user_roles,
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
        user_roles: Dict[str, str],
        page: int, 
        limit: int
    ) -> Dict[str, Any]:
        """Repository ensures group filtering in all queries."""
        
        # Base query with group filtering
        base_query = select(ExecutionHistory).where(
            ExecutionHistory.group_id.in_(group_ids)
        )
        
        # Additional filtering based on user role (if needed)
        # Viewers might see fewer fields, Users see own + shared, etc.
        
        # Count query with same group filtering
        count_query = select(func.count(ExecutionHistory.id)).where(
            ExecutionHistory.group_id.in_(group_ids)
        )
        
        # Both queries automatically respect group boundaries
        # No possibility of cross-group data access
```

#### 5. **Database-Level Isolation**
Every table includes group columns with indexed access:

```sql
-- All tables follow this pattern
ALTER TABLE execution_history 
ADD COLUMN group_id VARCHAR(100) NOT NULL,
ADD COLUMN created_by_email VARCHAR(255),
ADD INDEX idx_group_id (group_id),
ADD INDEX idx_created_by_email (created_by_email);

-- Queries are always group-scoped
SELECT * FROM execution_history 
WHERE group_id IN ('user_alice_company_com', 'dev_team') 
ORDER BY created_at DESC;
```

## Comprehensive Isolation Mechanisms

### AI Generation Services Isolation

All AI-powered generation services (agents, tasks, crews) include group context for complete isolation:

#### LLM Interaction Logging
```python
class AgentGenerationService:
    async def generate_agent(
        self, 
        prompt_text: str, 
        model: str = None, 
        tools: List[str] = None,
        group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Generate agent with group-aware logging."""
        
        # Generate agent configuration
        agent_config = await self._generate_agent_config(prompt_text, model)
        
        # Log interaction with group context
        await self._log_llm_interaction(
            endpoint='generate-agent',
            prompt=prompt_text,
            response=json.dumps(agent_config),
            model=model,
            group_context=group_context  # Group isolation for logs
        )
        
        return agent_config
```

#### LLM Log Isolation
Every AI interaction is logged with group boundaries:

```python
class LLMLogService:
    async def create_log(
        self,
        endpoint: str,
        prompt: str, 
        response: str,
        model: str,
        group_context: Optional[GroupContext] = None
    ) -> LLMLog:
        """Create LLM log with automatic group isolation."""
        
        log_data = {
            "endpoint": endpoint,
            "prompt": prompt,
            "response": response, 
            "model": model,
            "status": "success"
        }
        
        # Add group fields if context provided
        if group_context and group_context.primary_group_id:
            log_data["group_id"] = group_context.primary_group_id
            log_data["group_email"] = group_context.group_email
            
        # Log is automatically scoped to group
        return await self.repository.create(log_data)
```

### Execution Workflow Isolation

#### Execution History Isolation
```python
class ExecutionHistoryService:
    async def get_executions_with_group_filter(
        self, 
        group_context: GroupContext,
        status: Optional[str] = None,
        flow_id: Optional[str] = None
    ) -> List[ExecutionHistory]:
        """Get executions with comprehensive group filtering."""
        
        # Multiple filter criteria, all group-scoped
        return await self.repository.get_filtered_by_group(
            group_ids=group_context.group_ids,
            status=status,
            flow_id=flow_id,
            # Additional filters automatically respect group boundaries
        )
```

#### Execution Logs Isolation  
```python
class ExecutionLogsService:
    async def get_logs_for_execution(
        self,
        execution_id: str,
        group_context: GroupContext
    ) -> List[ExecutionLog]:
        """Get execution logs with group validation."""
        
        # First verify execution belongs to user's groups
        execution = await self.execution_repository.get_by_id_and_group(
            execution_id=execution_id,
            group_ids=group_context.group_ids
        )
        
        if not execution:
            raise PermissionError("Execution not found or not accessible")
            
        # Get logs only if execution is accessible
        return await self.logs_repository.get_by_execution_and_group(
            execution_id=execution_id,
            group_ids=group_context.group_ids
        )
```

### Scheduler Isolation

#### Scheduled Job Isolation
```python
class SchedulerService:
    async def create_schedule(
        self,
        schedule_data: ScheduleCreate,
        group_context: GroupContext
    ) -> Schedule:
        """Create schedule with group ownership."""
        
        # Automatically assign group ownership
        schedule_dict = schedule_data.model_dump()
        schedule_dict['group_id'] = group_context.primary_group_id
        schedule_dict['created_by_email'] = group_context.group_email
        
        return await self.repository.create_with_group(schedule_dict)
        
    async def get_user_schedules(
        self,
        group_context: GroupContext
    ) -> List[Schedule]:
        """Get schedules visible to user's groups."""
        
        return await self.repository.get_by_group_ids(
            group_ids=group_context.group_ids
        )
```

### Crew Management Isolation

#### Crew Access Control
```python
class CrewService:
    async def get_user_crews(
        self,
        group_context: GroupContext,
        include_shared: bool = True
    ) -> List[Crew]:
        """Get crews accessible to user with group filtering."""
        
        if include_shared:
            # User sees crews from all their assigned groups
            return await self.repository.get_by_group_ids(
                group_ids=group_context.group_ids
            )
        else:
            # User sees only crews they created
            return await self.repository.get_by_creator_and_group(
                created_by_email=group_context.group_email,
                group_ids=group_context.group_ids
            )
```

### Frontend Isolation Mechanisms

#### API Service Layer Isolation
```typescript
// Frontend services automatically include group headers
class ExecutionHistoryService {
    static async getExecutions(
        page: number = 1, 
        limit: number = 20
    ): Promise<PaginatedResponse<ExecutionRun>> {
        // Group context automatically extracted from headers
        // Backend filters results by user's accessible groups
        const response = await ApiService.get('/execution-history/', {
            params: { page, limit }
        });
        
        // Response contains only group-accessible data
        return response.data;
    }
}
```

#### Component-Level Data Isolation
```typescript
// Frontend components work with pre-filtered data
const ExecutionHistory: React.FC = () => {
    const [executions, setExecutions] = useState<ExecutionRun[]>([]);
    
    useEffect(() => {
        const loadExecutions = async () => {
            // Service returns only group-accessible executions
            const response = await ExecutionHistoryService.getExecutions();
            setExecutions(response.data);
        };
        
        loadExecutions();
    }, []);
    
    // Component automatically displays group-filtered data
    // No additional filtering needed in UI layer
    return (
        <div>
            {executions.map(execution => (
                <ExecutionCard key={execution.id} execution={execution} />
            ))}
        </div>
    );
};
```

### Data Creation Isolation

#### Automatic Group Assignment
```python
class TaskService:
    async def create_with_group(
        self,
        task_data: TaskCreate,
        group_context: GroupContext
    ) -> Task:
        """Create task with automatic group assignment."""
        
        # Convert Pydantic model to dict for manipulation
        task_dict = task_data.model_dump()
        
        # Automatically assign group ownership
        task_dict['group_id'] = group_context.primary_group_id
        task_dict['created_by_email'] = group_context.group_email
        
        # Create task with group isolation
        return await self.repository.create(task_dict)
```

#### Group Validation on Updates
```python
class AgentService:
    async def update_agent(
        self,
        agent_id: str,
        agent_update: AgentUpdate,
        group_context: GroupContext
    ) -> Agent:
        """Update agent with group ownership validation."""
        
        # First verify agent belongs to user's groups
        existing_agent = await self.repository.get_by_id_and_group(
            agent_id=agent_id,
            group_ids=group_context.group_ids
        )
        
        if not existing_agent:
            raise PermissionError("Agent not found or not accessible")
            
        # Update only if group validation passes
        return await self.repository.update(agent_id, agent_update.model_dump())
```

### Cross-Service Isolation

#### Service-to-Service Group Propagation
```python
class DispatcherService:
    async def dispatch(
        self, 
        request: DispatcherRequest, 
        group_context: GroupContext
    ) -> Dict[str, Any]:
        """Dispatch with group context propagation."""
        
        if dispatcher_response.intent == IntentType.GENERATE_AGENT:
            # Pass group context to agent generation
            generation_result = await self.agent_service.generate_agent(
                prompt_text=request.message,
                model=request.model,
                tools=request.tools,
                group_context=group_context  # Group context propagated
            )
            
        elif dispatcher_response.intent == IntentType.GENERATE_TASK:
            # Pass group context to task generation
            task_request = TaskGenerationRequest(
                text=request.message,
                model=request.model
            )
            generation_result = await self.task_service.generate_and_save_task(
                task_request, 
                group_context  # Group context propagated
            )
```

## Individual vs Group Permissions

### Individual Mode
- User has full admin rights over their private group
- No collaboration features available
- Complete isolation from other users
- All RBAC privileges available within personal scope

### Group Mode
- Permissions determined by role within each group
- Can have different roles in different groups
- Access to shared group resources
- Role-based privilege restrictions apply

## Group Management

### Group Creation

**Who Can Create:**
- System administrators
- Users with appropriate permissions

**Process:**
1. Admin creates group with unique identifier
2. Group gets assigned domain/namespace
3. Initial admin user assigned
4. Group becomes available for user assignment

### User Assignment

**Security Requirements:**
- Must be performed by group admin or manager
- Explicit role assignment required
- Audit trail maintained for all changes

**Assignment Process:**
```python
# Secure user assignment
group_service.assign_user_to_group(
    group_id="dev_team",
    user_email="alice@company.com", 
    role="user",
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

**Mock Development Mode:**
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
    else:
        # Group mode - shared groups
        group_ids = [group.id for group in user_groups]
    
    return GroupContext(
        group_ids=group_ids,
        group_email=email,
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

#### 3. **Privilege Escalation**
- **Threat:** User gaining unauthorized permissions within group
- **Mitigation:** Role-based access control with explicit assignments
- **Detection:** Permission checks on every sensitive operation

#### 4. **Admin Abuse**
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
    role VARCHAR(50) NOT NULL DEFAULT 'user',
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

### Migration & Upgrade Path

#### From Domain-Based to Hybrid Model
1. **Backup existing data** with current group assignments
2. **Run group migration** to update individual users
3. **Create groups** for collaborative teams
4. **Migrate team users** to appropriate groups
5. **Validate data isolation** post-migration

#### Individual User Migration
```python
async def migrate_to_individual_groups(user_email: str):
    """Migrate user from domain to individual groups."""
    old_group_id = generate_group_id(extract_domain(user_email))
    new_group_id = generate_individual_group_id(user_email)
    
    # Update all user's data to new group ID
    await update_user_group_assignments(old_group_id, new_group_id, user_email)
```

---

## Summary

The Kasal security model provides robust multi-group isolation with flexible collaboration capabilities. By automatically adapting between individual and group modes, it ensures both privacy and productivity while maintaining strong security boundaries. The hybrid approach scales from individual developers to large collaborative teams without compromising security or usability.

For technical implementation details, see the source code in:
- `/src/utils/user_context.py` - Group context management
- `/src/core/dependencies.py` - Security dependencies
- `/src/services/group_service.py` - Group management
- `/src/api/group_router.py` - Admin API endpoints