# Security Model

## Overview

Kasal implements a comprehensive hybrid multi-tenant security model that provides both individual privacy and team collaboration capabilities. The system automatically adapts security boundaries based on group membership without requiring user configuration.

## Table of Contents

- [Core Security Principles](#core-security-principles)
- [Tenancy Models](#tenancy-models)
- [Data Isolation](#data-isolation)
- [Access Control](#access-control)
- [Group Management](#group-management)
- [Authentication & Authorization](#authentication--authorization)
- [Security Guarantees](#security-guarantees)
- [Threat Model](#threat-model)
- [Implementation Details](#implementation-details)

## Core Security Principles

### 1. **Privacy by Default**
- Users without group assignments get complete data isolation
- Individual tenant IDs ensure no cross-user data access
- Zero trust approach - access must be explicitly granted

### 2. **Explicit Group Membership**
- Group access requires admin-managed assignments
- No automatic group enrollment based on email domains
- Clear audit trail of all group assignments

### 3. **Least Privilege Access**
- Role-based permissions within groups
- Users only see data from their assigned groups
- Granular permission system (Admin, Manager, User, Viewer)

### 4. **Defense in Depth**
- Multiple layers of security controls
- Database-level tenant isolation
- Application-level access controls
- API-level authentication and authorization

## Tenancy Models

### Individual Tenancy Mode

**When Applied:**
- User is not assigned to any groups
- Group lookup fails or returns empty results
- Fallback security mode for maximum isolation

**Security Characteristics:**
```
User: alice@company.com
Tenant ID: user_alice_company_com
Access Scope: Only Alice's data
```

**Data Isolation:**
- Execution history: Only Alice's workflows
- Agents & Tasks: Only Alice's creations
- Crews: Only Alice's team configurations
- Logs: Only Alice's execution logs

### Group Tenancy Mode

**When Applied:**
- User is assigned to one or more groups
- Admin has explicitly granted group membership
- User belongs to active groups

**Security Characteristics:**
```
User: bob@company.com
Groups: ["dev_team", "qa_team"]
Tenant IDs: ["dev_team", "qa_team"]
Access Scope: Data from both teams
```

**Data Isolation:**
- Execution history: From all assigned groups
- Agents & Tasks: Shared team resources
- Crews: Team configurations
- Logs: Team execution logs

## Data Isolation

### Database-Level Isolation

All major data entities include tenant isolation with automatic filtering at every data access point:

```sql
-- Example: Execution History Table
CREATE TABLE execution_history (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(100) NOT NULL,  -- Isolation key
    tenant_email VARCHAR(255),        -- Audit trail
    job_id UUID,
    status VARCHAR(50),
    created_at TIMESTAMP,
    -- ... other fields
    INDEX idx_tenant_id (tenant_id)   -- Performance optimization
);
```

**Comprehensive Isolated Entities:**

#### Core Workflow Components
- `execution_history` - All workflow execution records with tenant isolation
- `execution_logs` - Detailed execution logs filtered by tenant
- `flow_executions` - Flow execution instances with tenant boundaries
- `agents` - AI agent definitions scoped to tenant
- `tasks` - Task configurations with tenant access control
- `crews` - Team configurations isolated by tenant
- `flows` - Workflow definitions with tenant ownership
- `schedules` - Scheduled job executions with tenant filtering

#### Configuration & Templates
- `templates` - Reusable prompt templates scoped to tenant
- `tools` - Custom tool definitions with tenant access
- `model_configs` - LLM model configurations per tenant
- `engine_configs` - Execution engine settings by tenant

#### Logging & Monitoring
- `llmlog` - **NEW**: LLM API interaction logs with tenant isolation
- `execution_trace` - Detailed execution traces filtered by tenant
- `api_logs` - API access logs with tenant context

#### User Management
- `tenants` - Tenant definitions and metadata
- `tenant_users` - User-to-tenant assignments with roles
- `users` - User profiles with tenant associations

### Query-Level Filtering

All data queries automatically include tenant filtering:

```python
# Individual user - single tenant
WHERE tenant_id = 'user_alice_company_com'

# Group user - multiple tenants
WHERE tenant_id IN ('dev_team', 'qa_team')
```

### Multi-Layer Isolation Architecture

The system implements isolation at multiple levels to ensure comprehensive data protection:

#### 1. **Request-Level Isolation**
Every HTTP request includes tenant context extraction:

```python
@dataclass
class TenantContext:
    tenant_ids: List[str]           # All accessible tenant IDs
    tenant_email: str               # User's email for audit
    email_domain: str               # Email domain
    user_id: Optional[str]          # User identifier
    access_token: Optional[str]     # Authentication token

    @property
    def primary_tenant_id(self) -> str:
        """Primary tenant for creating new data."""
        return self.tenant_ids[0] if self.tenant_ids else None
```

#### 2. **API Endpoint Isolation**
All API endpoints automatically enforce tenant boundaries:

```python
# Every endpoint that accesses data includes tenant dependency
@router.get("/executions/")
async def get_executions(
    tenant_context: TenantContextDep,  # Automatic tenant injection
    page: int = 1,
    limit: int = 20
):
    # Service automatically filters by tenant_context.tenant_ids
    return await execution_service.get_executions_paginated(
        tenant_context=tenant_context,
        page=page, 
        limit=limit
    )
```

#### 3. **Service-Level Isolation**
All business logic services respect tenant boundaries:

```python
class ExecutionHistoryService:
    async def get_executions_paginated(
        self, 
        tenant_context: TenantContext,
        page: int = 1, 
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get executions with automatic tenant filtering."""
        # Automatic tenant ID filtering
        return await self.repository.get_paginated_by_tenant(
            tenant_ids=tenant_context.tenant_ids,
            page=page,
            limit=limit
        )
```

#### 4. **Repository-Level Isolation**
Data access layer enforces tenant filtering at query level:

```python
class ExecutionHistoryRepository:
    async def get_paginated_by_tenant(
        self, 
        tenant_ids: List[str], 
        page: int, 
        limit: int
    ) -> Dict[str, Any]:
        """Repository ensures tenant filtering in all queries."""
        
        # Base query with tenant filtering
        base_query = select(ExecutionHistory).where(
            ExecutionHistory.tenant_id.in_(tenant_ids)
        )
        
        # Count query with same tenant filtering
        count_query = select(func.count(ExecutionHistory.id)).where(
            ExecutionHistory.tenant_id.in_(tenant_ids)
        )
        
        # Both queries automatically respect tenant boundaries
        # No possibility of cross-tenant data access
```

#### 5. **Database-Level Isolation**
Every table includes tenant columns with indexed access:

```sql
-- All tables follow this pattern
ALTER TABLE execution_history 
ADD COLUMN tenant_id VARCHAR(100) NOT NULL,
ADD COLUMN tenant_email VARCHAR(255),
ADD INDEX idx_tenant_id (tenant_id),
ADD INDEX idx_tenant_email (tenant_email);

-- Queries are always tenant-scoped
SELECT * FROM execution_history 
WHERE tenant_id IN ('user_alice_company_com', 'dev_team') 
ORDER BY created_at DESC;
```

## Comprehensive Isolation Mechanisms

### AI Generation Services Isolation

All AI-powered generation services (agents, tasks, crews) include tenant context for complete isolation:

#### LLM Interaction Logging
```python
class AgentGenerationService:
    async def generate_agent(
        self, 
        prompt_text: str, 
        model: str = None, 
        tools: List[str] = None,
        tenant_context: Optional[TenantContext] = None
    ) -> Dict[str, Any]:
        """Generate agent with tenant-aware logging."""
        
        # Generate agent configuration
        agent_config = await self._generate_agent_config(prompt_text, model)
        
        # Log interaction with tenant context
        await self._log_llm_interaction(
            endpoint='generate-agent',
            prompt=prompt_text,
            response=json.dumps(agent_config),
            model=model,
            tenant_context=tenant_context  # Tenant isolation for logs
        )
        
        return agent_config
```

#### LLM Log Isolation
Every AI interaction is logged with tenant boundaries:

```python
class LLMLogService:
    async def create_log(
        self,
        endpoint: str,
        prompt: str, 
        response: str,
        model: str,
        tenant_context: Optional[TenantContext] = None
    ) -> LLMLog:
        """Create LLM log with automatic tenant isolation."""
        
        log_data = {
            "endpoint": endpoint,
            "prompt": prompt,
            "response": response, 
            "model": model,
            "status": "success"
        }
        
        # Add tenant fields if context provided
        if tenant_context and tenant_context.primary_tenant_id:
            log_data["tenant_id"] = tenant_context.primary_tenant_id
            log_data["tenant_email"] = tenant_context.tenant_email
            
        # Log is automatically scoped to tenant
        return await self.repository.create(log_data)
```

### Execution Workflow Isolation

#### Execution History Isolation
```python
class ExecutionHistoryService:
    async def get_executions_with_tenant_filter(
        self, 
        tenant_context: TenantContext,
        status: Optional[str] = None,
        flow_id: Optional[str] = None
    ) -> List[ExecutionHistory]:
        """Get executions with comprehensive tenant filtering."""
        
        # Multiple filter criteria, all tenant-scoped
        return await self.repository.get_filtered_by_tenant(
            tenant_ids=tenant_context.tenant_ids,
            status=status,
            flow_id=flow_id,
            # Additional filters automatically respect tenant boundaries
        )
```

#### Execution Logs Isolation  
```python
class ExecutionLogsService:
    async def get_logs_for_execution(
        self,
        execution_id: str,
        tenant_context: TenantContext
    ) -> List[ExecutionLog]:
        """Get execution logs with tenant validation."""
        
        # First verify execution belongs to user's tenants
        execution = await self.execution_repository.get_by_id_and_tenant(
            execution_id=execution_id,
            tenant_ids=tenant_context.tenant_ids
        )
        
        if not execution:
            raise PermissionError("Execution not found or not accessible")
            
        # Get logs only if execution is accessible
        return await self.logs_repository.get_by_execution_and_tenant(
            execution_id=execution_id,
            tenant_ids=tenant_context.tenant_ids
        )
```

### Scheduler Isolation

#### Scheduled Job Isolation
```python
class SchedulerService:
    async def create_schedule(
        self,
        schedule_data: ScheduleCreate,
        tenant_context: TenantContext
    ) -> Schedule:
        """Create schedule with tenant ownership."""
        
        # Automatically assign tenant ownership
        schedule_dict = schedule_data.model_dump()
        schedule_dict['tenant_id'] = tenant_context.primary_tenant_id
        schedule_dict['created_by_email'] = tenant_context.tenant_email
        
        return await self.repository.create_with_tenant(schedule_dict)
        
    async def get_user_schedules(
        self,
        tenant_context: TenantContext
    ) -> List[Schedule]:
        """Get schedules visible to user's tenants."""
        
        return await self.repository.get_by_tenant_ids(
            tenant_ids=tenant_context.tenant_ids
        )
```

### Crew Management Isolation

#### Crew Access Control
```python
class CrewService:
    async def get_user_crews(
        self,
        tenant_context: TenantContext,
        include_shared: bool = True
    ) -> List[Crew]:
        """Get crews accessible to user with tenant filtering."""
        
        if include_shared:
            # User sees crews from all their assigned tenants
            return await self.repository.get_by_tenant_ids(
                tenant_ids=tenant_context.tenant_ids
            )
        else:
            # User sees only crews they created
            return await self.repository.get_by_creator_and_tenant(
                created_by_email=tenant_context.tenant_email,
                tenant_ids=tenant_context.tenant_ids
            )
```

### Frontend Isolation Mechanisms

#### API Service Layer Isolation
```typescript
// Frontend services automatically include tenant headers
class ExecutionHistoryService {
    static async getExecutions(
        page: number = 1, 
        limit: number = 20
    ): Promise<PaginatedResponse<ExecutionRun>> {
        // Tenant context automatically extracted from headers
        // Backend filters results by user's accessible tenants
        const response = await ApiService.get('/execution-history/', {
            params: { page, limit }
        });
        
        // Response contains only tenant-accessible data
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
            // Service returns only tenant-accessible executions
            const response = await ExecutionHistoryService.getExecutions();
            setExecutions(response.data);
        };
        
        loadExecutions();
    }, []);
    
    // Component automatically displays tenant-filtered data
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

#### Automatic Tenant Assignment
```python
class TaskService:
    async def create_with_tenant(
        self,
        task_data: TaskCreate,
        tenant_context: TenantContext
    ) -> Task:
        """Create task with automatic tenant assignment."""
        
        # Convert Pydantic model to dict for manipulation
        task_dict = task_data.model_dump()
        
        # Automatically assign tenant ownership
        task_dict['tenant_id'] = tenant_context.primary_tenant_id
        task_dict['created_by_email'] = tenant_context.tenant_email
        
        # Create task with tenant isolation
        return await self.repository.create(task_dict)
```

#### Tenant Validation on Updates
```python
class AgentService:
    async def update_agent(
        self,
        agent_id: str,
        agent_update: AgentUpdate,
        tenant_context: TenantContext
    ) -> Agent:
        """Update agent with tenant ownership validation."""
        
        # First verify agent belongs to user's tenants
        existing_agent = await self.repository.get_by_id_and_tenant(
            agent_id=agent_id,
            tenant_ids=tenant_context.tenant_ids
        )
        
        if not existing_agent:
            raise PermissionError("Agent not found or not accessible")
            
        # Update only if tenant validation passes
        return await self.repository.update(agent_id, agent_update.model_dump())
```

### Cross-Service Isolation

#### Service-to-Service Tenant Propagation
```python
class DispatcherService:
    async def dispatch(
        self, 
        request: DispatcherRequest, 
        tenant_context: TenantContext
    ) -> Dict[str, Any]:
        """Dispatch with tenant context propagation."""
        
        if dispatcher_response.intent == IntentType.GENERATE_AGENT:
            # Pass tenant context to agent generation
            generation_result = await self.agent_service.generate_agent(
                prompt_text=request.message,
                model=request.model,
                tools=request.tools,
                tenant_context=tenant_context  # Tenant context propagated
            )
            
        elif dispatcher_response.intent == IntentType.GENERATE_TASK:
            # Pass tenant context to task generation
            task_request = TaskGenerationRequest(
                text=request.message,
                model=request.model
            )
            generation_result = await self.task_service.generate_and_save_task(
                task_request, 
                tenant_context  # Tenant context propagated
            )
```

## Access Control

### Role-Based Access Control (RBAC)

#### Role Hierarchy

1. **Admin** - Full administrative control
   - Manage group membership
   - Delete groups and users
   - Access all group data
   - Modify group settings

2. **Manager** - Team management capabilities
   - Manage team workflows
   - Invite/remove team members
   - View team execution history
   - Modify team resources

3. **User** - Standard workflow execution
   - Create and execute workflows
   - View own and team execution history
   - Use team shared resources
   - Create personal agents/tasks

4. **Viewer** - Read-only access
   - View team workflows
   - View execution history
   - Cannot modify or execute
   - Cannot create new resources

#### Permission Matrix

| Resource | Admin | Manager | User | Viewer |
|----------|-------|---------|------|--------|
| Group Management | ✓ | ✗ | ✗ | ✗ |
| User Assignment | ✓ | ✓ | ✗ | ✗ |
| Workflow Creation | ✓ | ✓ | ✓ | ✗ |
| Workflow Execution | ✓ | ✓ | ✓ | ✗ |
| Execution History | ✓ | ✓ | ✓ | ✓ |
| Agent Creation | ✓ | ✓ | ✓ | ✗ |
| Tool Management | ✓ | ✓ | Limited | ✗ |

### Individual vs Group Permissions

#### Individual Mode
- User has full admin rights over their private tenant
- No collaboration features available
- Complete isolation from other users

#### Group Mode
- Permissions determined by role within each group
- Can have different roles in different groups
- Access to shared group resources

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
tenant_service.assign_user_to_tenant(
    tenant_id="dev_team",
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
2. **Tenant Resolution:** System determines user's tenant context
3. **Authorization:** Request permissions validated against tenant access
4. **Data Filtering:** Results filtered to accessible tenants only

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

### Tenant Context Resolution

```python
async def extract_tenant_context(request: Request) -> TenantContext:
    email = request.headers.get('X-Forwarded-Email')
    
    # Look up user's group memberships
    user_groups = await get_user_tenant_memberships(email)
    
    if not user_groups:
        # Individual mode - private tenant
        tenant_ids = [generate_individual_tenant_id(email)]
    else:
        # Group mode - shared tenants
        tenant_ids = [group.id for group in user_groups]
    
    return TenantContext(
        tenant_ids=tenant_ids,
        tenant_email=email,
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

### Tenant ID Generation

#### Individual Tenants
```python
def generate_individual_tenant_id(email: str) -> str:
    """Generate unique tenant ID for individual user."""
    sanitized = email.replace("@", "_").replace(".", "_")
    return f"user_{sanitized}".lower()

# Examples:
# alice@company.com → user_alice_company_com
# bob.smith@startup.io → user_bob_smith_startup_io
```

#### Group Tenants
```python
def generate_group_tenant_id(domain: str) -> str:
    """Generate tenant ID for group."""
    return domain.replace(".", "_").replace("-", "_").lower()

# Examples:
# dev-team → dev_team
# marketing.team → marketing_team
```

### Database Schema Security

#### Tenant Columns
All major tables include:
```sql
tenant_id VARCHAR(100) NOT NULL,     -- Primary isolation key
tenant_email VARCHAR(255),           -- Audit trail
INDEX idx_tenant_id (tenant_id),     -- Query performance
INDEX idx_tenant_email (tenant_email) -- Audit queries
```

#### Foreign Key Constraints
```sql
-- Group membership table
CREATE TABLE tenant_users (
    id VARCHAR(100) PRIMARY KEY,
    tenant_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'user',
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_tenant (user_id, tenant_id)
);
```

### API Security Implementation

#### Tenant Context Dependency
```python
async def get_tenant_context(request: Request) -> TenantContext:
    """Extract and validate tenant context from request."""
    context = await extract_tenant_context_from_request(request)
    if not context or not context.is_valid():
        raise HTTPException(401, "Invalid tenant context")
    return context

# Usage in endpoints
@router.get("/executions/")
async def get_executions(
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)]
):
    return await execution_service.get_executions(
        tenant_ids=tenant_context.tenant_ids
    )
```

#### Query Filtering
```python
async def get_executions(self, tenant_ids: List[str]) -> List[Execution]:
    """Get executions with automatic tenant filtering."""
    stmt = select(ExecutionHistory).where(
        ExecutionHistory.tenant_id.in_(tenant_ids)
    ).order_by(ExecutionHistory.created_at.desc())
    
    result = await session.execute(stmt)
    return result.scalars().all()
```

### Migration & Upgrade Path

#### From Domain-Based to Hybrid Model
1. **Backup existing data** with current tenant assignments
2. **Run tenant migration** to update individual users
3. **Create groups** for collaborative teams
4. **Migrate team users** to appropriate groups
5. **Validate data isolation** post-migration

#### Individual User Migration
```python
async def migrate_to_individual_tenancy(user_email: str):
    """Migrate user from domain to individual tenancy."""
    old_tenant_id = generate_tenant_id(extract_domain(user_email))
    new_tenant_id = generate_individual_tenant_id(user_email)
    
    # Update all user's data to new tenant ID
    await update_user_tenant_assignments(old_tenant_id, new_tenant_id, user_email)
```

---

## Summary

The Kasal security model provides robust multi-tenant isolation with flexible collaboration capabilities. By automatically adapting between individual and group modes, it ensures both privacy and productivity while maintaining strong security boundaries. The hybrid approach scales from individual developers to large collaborative teams without compromising security or usability.

For technical implementation details, see the source code in:
- `/src/utils/user_context.py` - Tenant context management
- `/src/core/dependencies.py` - Security dependencies
- `/src/services/tenant_service.py` - Group management
- `/src/api/tenant_router.py` - Admin API endpoints