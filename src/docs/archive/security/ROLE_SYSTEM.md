# Kasal Role System - Detailed Specification

## Overview
Kasal implements a two-tier permission system:
1. **User-Level Permissions**: Properties on the user account for system-wide or personal capabilities
2. **Teamspace Roles**: Role assignments within each teamspace context

## Role Hierarchy

```
┌─────────────────────────────────────────────┐
│           SYSTEM ADMIN                       │
│  (User Property: is_system_admin)           │
│  • Manages entire platform                  │
│  • Grants user-level permissions            │
│  • Has Admin role in all teamspaces         │
└─────────────────────────────────────────────┘
                    │
    ┌───────────────┴──────────────┐
    ▼                              ▼
┌─────────────────────────┐  ┌─────────────────────────┐
│  PERSONAL SPACE         │  │  SHARED TEAMSPACES      │
│      MANAGER            │  │                         │
│ (User Property:         │  │  Per-Teamspace Roles:   │
│  is_personal_workspace  │  │  • Admin                │
│  _manager)              │  │  • Editor               │
│                         │  │  • Operator             │
│ Gets Admin role in      │  │                         │
│ Personal Space          │  │ Assigned explicitly     │
└─────────────────────────┘  └─────────────────────────┘
```

## User-Level Permissions (Properties)

### 1. System Admin (`is_system_admin`)
**Purpose**: Platform-wide administration and management

**Database Field**: `users.is_system_admin` (Boolean, default: False)

**Capabilities:**
- Configure system-wide settings (models, tools)
- Create and delete any teamspace
- Manage all users and their permissions
- Grant/revoke user-level permissions
- Automatic Admin role in every teamspace
- View and manage all teamspace configurations

**How to Grant:**
- Only existing System Admins can grant this
- Cannot be self-assigned
- First admin set via database seed or manual DB update

### 2. Personal Space Manager (`is_personal_workspace_manager`)
**Purpose**: Allow users to fully configure their Personal Space

**Database Field**: `users.is_personal_workspace_manager` (Boolean, default: False)

**Capabilities:**
- Configure Databricks integration in the Personal Space
- Set up memory backend configuration
- Manage volumes and storage settings
- Configure knowledge base settings
- Gets Admin role (not Editor) in the Personal Space

**How to Grant:**
- System Admin grants via user management interface
- Cannot be self-assigned
- Typically granted to power users, developers, data scientists

## Teamspace Roles

### 1. Admin
**Full teamspace control** - Complete teamspace management

**Core Capabilities:**
- All Editor capabilities
- Configure teamspace-specific settings:
  - Databricks connection and warehouses
  - Memory backend providers
  - Volume storage paths
  - Knowledge base configuration
- Manage teamspace members (shared teamspaces only)
- Delete teamspace resources
- View all teamspace activity

**Assignment:**
- Shared teamspace: Explicitly assigned by existing Admin
- Personal Space: Automatic if `is_personal_workspace_manager`
- System Admin: Always has Admin role everywhere

### 2. Editor
**Workflow developer** - Build and execute workflows

**Core Capabilities:**
- Create, edit, delete agents
- Create, edit, delete tasks
- Create, edit, delete crews
- Execute workflows
- View execution history and logs
- Manage API keys
- Cannot configure teamspace settings

**Assignment:**
- Shared teamspace: Explicitly assigned
- Personal Space: Default role (if not Personal Space Manager)

### 3. Operator
**Execution user** - Execute and monitor only

**Core Capabilities:**
- Execute existing workflows
- Monitor execution status
- View logs and results
- Cannot create or modify anything
- Cannot configure anything

**Assignment:**
- Shared teamspace: Explicitly assigned
- Personal Space: Not typically used

## Permission Resolution Algorithm

```python
def get_effective_role(user, teamspace):
    # System Admin always gets Admin role
    if user.is_system_admin:
        return "Admin"

    # Personal Space logic
    if teamspace.is_personal_teamspace(user):
        if user.is_personal_workspace_manager:
            return "Admin"
        else:
            return "Editor"

    # Shared teamspace - use assigned role
    return teamspace.get_user_role(user) or None
```

## Permission Matrix

| Action | System Admin | Personal Space Manager | Teamspace Admin | Editor | Operator |
|--------|--------------|---------------------------|-----------------|--------|----------|
| **System Management** |
| Configure Models/Tools | ✅ | ❌ | ❌ | ❌ | ❌ |
| Manage All Teamspaces | ✅ | ❌ | ❌ | ❌ | ❌ |
| Grant User Permissions | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Personal Space** |
| Configure Own Databricks | ✅ | ✅ | N/A | ❌ | ❌ |
| Configure Own Memory | ✅ | ✅ | N/A | ❌ | ❌ |
| Configure Own Volumes | ✅ | ✅ | N/A | ❌ | ❌ |
| **Shared Teamspace** |
| Configure Team Settings | ✅ | ❌ | ✅ | ❌ | ❌ |
| Manage Team Members | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Workflows** |
| Create/Edit Workflows | ✅ | ✅* | ✅ | ✅ | ❌ |
| Execute Workflows | ✅ | ✅* | ✅ | ✅ | ✅ |
| Delete Workflows | ✅ | ✅* | ✅ | ✅ | ❌ |

*In their Personal Space only, based on their role there

## Database Schema

### Users Table
```sql
users:
  - id: UUID
  - email: String
  - is_system_admin: Boolean (default: False)
  - is_personal_workspace_manager: Boolean (default: False)
```

### Group Users Table (Teamspace Membership)
```sql
group_users:
  - user_id: UUID (FK -> users.id)
  - group_id: String (FK -> groups.id)
  - role: Enum["admin", "editor", "operator"]
```

## API Implementation

### Backend (Python/FastAPI)
```python
# Check teamspace role
@require_roles(["admin"])  # Teamspace admin only
@require_roles(["admin", "editor"])  # Admin or editor

# Check if teamspace admin (includes Personal Space logic)
from core.permissions import is_workspace_admin
if is_workspace_admin(group_context):
    # Can configure the teamspace

# Check system admin
if user.is_system_admin:
    # Can manage system
```

### Frontend (TypeScript/React)
```typescript
// Check teamspace role
const { userRole, isAdmin, canEdit } = usePermissionStore();

// Check if can configure the teamspace
const isWorkspaceAdmin = userRole === 'admin';
const canConfigure = isWorkspaceAdmin || isPersonalWorkspaceManager;

// Check system admin
if (isSystemAdmin) {
  // Show system management UI
}
```

## Common Scenarios

### Scenario 1: New User Joins
1. User authenticates via OAuth
2. System creates user account
3. Personal Space `user_[email]` created automatically
4. User gets Editor role in their Personal Space (can work, not configure)
5. User can be invited to shared teamspaces

### Scenario 2: User Wants to Configure Their Personal Space
1. User requests configuration access
2. System Admin reviews request
3. System Admin grants `is_personal_workspace_manager`
4. User now has Admin role in their Personal Space
5. User can configure Databricks, memory, volumes

### Scenario 3: Team Collaboration
1. Teamspace Admin creates a shared teamspace
2. Admin invites users with specific roles:
   - Admin: Co-administrators
   - Editor: Developers who build workflows
   - Operator: Production support who run workflows
3. Each user has their assigned role in the shared teamspace
4. Complete isolation from other teamspaces

### Scenario 4: System Administration
1. User is granted `is_system_admin` by existing admin
2. User can now:
   - Configure system-wide models and tools
   - Manage all teamspaces
   - Grant permissions to other users
   - Has Admin role in every teamspace automatically

## Security Considerations

1. **Least Privilege**: Users start with minimal permissions
2. **Explicit Grants**: All permissions must be explicitly granted
3. **No Self-Elevation**: Users cannot grant themselves permissions
4. **Audit Trail**: All permission changes logged
5. **Teamspace Isolation**: Complete data isolation between teamspaces
6. **Role Immutability**: Roles cannot be modified while active
7. **Session Management**: Role changes require re-authentication
