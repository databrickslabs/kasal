# Kasal Role System - Detailed Specification

## Overview
Kasal implements a two-tier permission system:
1. **User-Level Permissions**: Properties on the user account for system-wide or personal capabilities
2. **Workspace Roles**: Role assignments within each workspace context

## Role Hierarchy

```
┌─────────────────────────────────────────────┐
│           SYSTEM ADMIN                       │
│  (User Property: is_system_admin)           │
│  • Manages entire platform                  │
│  • Grants user-level permissions            │
│  • Has Admin role in all workspaces         │
└─────────────────────────────────────────────┘
                    │
    ┌───────────────┴──────────────┐
    ▼                              ▼
┌─────────────────────────┐  ┌─────────────────────────┐
│  PERSONAL WORKSPACE     │  │  TEAM WORKSPACES        │
│      MANAGER            │  │                         │
│ (User Property:         │  │  Per-Workspace Roles:   │
│  is_personal_workspace  │  │  • Admin                │
│  _manager)              │  │  • Editor               │
│                         │  │  • Operator             │
│ Gets Admin role in      │  │                         │
│ personal workspace      │  │ Assigned explicitly     │
└─────────────────────────┘  └─────────────────────────┘
```

## User-Level Permissions (Properties)

### 1. System Admin (`is_system_admin`)
**Purpose**: Platform-wide administration and management

**Database Field**: `users.is_system_admin` (Boolean, default: False)

**Capabilities:**
- Configure system-wide settings (models, tools)
- Create and delete any workspace
- Manage all users and their permissions
- Grant/revoke user-level permissions
- Automatic Admin role in every workspace
- View and manage all workspace configurations

**How to Grant:**
- Only existing System Admins can grant this
- Cannot be self-assigned
- First admin set via database seed or manual DB update

### 2. Personal Workspace Manager (`is_personal_workspace_manager`)
**Purpose**: Allow users to fully configure their personal workspace

**Database Field**: `users.is_personal_workspace_manager` (Boolean, default: False)

**Capabilities:**
- Configure Databricks integration in personal workspace
- Set up memory backend configuration
- Manage volumes and storage settings
- Configure knowledge base settings
- Gets Admin role (not Editor) in personal workspace

**How to Grant:**
- System Admin grants via user management interface
- Cannot be self-assigned
- Typically granted to power users, developers, data scientists

## Workspace Roles

### 1. Admin
**Full workspace control** - Complete workspace management

**Core Capabilities:**
- All Editor capabilities
- Configure workspace-specific settings:
  - Databricks connection and warehouses
  - Memory backend providers
  - Volume storage paths
  - Knowledge base configuration
- Manage workspace members (team workspaces only)
- Delete workspace resources
- View all workspace activity

**Assignment:**
- Team workspace: Explicitly assigned by existing Admin
- Personal workspace: Automatic if `is_personal_workspace_manager`
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
- Cannot configure workspace settings

**Assignment:**
- Team workspace: Explicitly assigned
- Personal workspace: Default role (if not Personal Workspace Manager)

### 3. Operator
**Execution user** - Execute and monitor only

**Core Capabilities:**
- Execute existing workflows
- Monitor execution status
- View logs and results
- Cannot create or modify anything
- Cannot configure anything

**Assignment:**
- Team workspace: Explicitly assigned
- Personal workspace: Not typically used

## Permission Resolution Algorithm

```python
def get_effective_role(user, workspace):
    # System Admin always gets Admin role
    if user.is_system_admin:
        return "Admin"

    # Personal workspace logic
    if workspace.is_personal_workspace(user):
        if user.is_personal_workspace_manager:
            return "Admin"
        else:
            return "Editor"

    # Team workspace - use assigned role
    return workspace.get_user_role(user) or None
```

## Permission Matrix

| Action | System Admin | Personal Workspace Manager | Workspace Admin | Editor | Operator |
|--------|--------------|---------------------------|-----------------|--------|----------|
| **System Management** |
| Configure Models/Tools | ✅ | ❌ | ❌ | ❌ | ❌ |
| Manage All Workspaces | ✅ | ❌ | ❌ | ❌ | ❌ |
| Grant User Permissions | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Personal Workspace** |
| Configure Own Databricks | ✅ | ✅ | N/A | ❌ | ❌ |
| Configure Own Memory | ✅ | ✅ | N/A | ❌ | ❌ |
| Configure Own Volumes | ✅ | ✅ | N/A | ❌ | ❌ |
| **Team Workspace** |
| Configure Team Settings | ✅ | ❌ | ✅ | ❌ | ❌ |
| Manage Team Members | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Workflows** |
| Create/Edit Workflows | ✅ | ✅* | ✅ | ✅ | ❌ |
| Execute Workflows | ✅ | ✅* | ✅ | ✅ | ✅ |
| Delete Workflows | ✅ | ✅* | ✅ | ✅ | ❌ |

*In personal workspace only, based on their role there

## Database Schema

### Users Table
```sql
users:
  - id: UUID
  - email: String
  - is_system_admin: Boolean (default: False)
  - is_personal_workspace_manager: Boolean (default: False)
```

### Group Users Table (Workspace Membership)
```sql
group_users:
  - user_id: UUID (FK -> users.id)
  - group_id: String (FK -> groups.id)
  - role: Enum["admin", "editor", "operator"]
```

## API Implementation

### Backend (Python/FastAPI)
```python
# Check workspace role
@require_roles(["admin"])  # Workspace admin only
@require_roles(["admin", "editor"])  # Admin or editor

# Check if workspace admin (includes personal workspace logic)
from core.permissions import is_workspace_admin
if is_workspace_admin(group_context):
    # Can configure workspace

# Check system admin
if user.is_system_admin:
    # Can manage system
```

### Frontend (TypeScript/React)
```typescript
// Check workspace role
const { userRole, isAdmin, canEdit } = usePermissionStore();

// Check if can configure workspace
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
3. Personal workspace `user_[email]` created automatically
4. User gets Editor role in personal workspace (can work, not configure)
5. User can be invited to team workspaces

### Scenario 2: User Wants to Configure Personal Workspace
1. User requests configuration access
2. System Admin reviews request
3. System Admin grants `is_personal_workspace_manager`
4. User now has Admin role in personal workspace
5. User can configure Databricks, memory, volumes

### Scenario 3: Team Collaboration
1. Workspace Admin creates team workspace
2. Admin invites users with specific roles:
   - Admin: Co-administrators
   - Editor: Developers who build workflows
   - Operator: Production support who run workflows
3. Each user has their assigned role in team workspace
4. Complete isolation from other workspaces

### Scenario 4: System Administration
1. User is granted `is_system_admin` by existing admin
2. User can now:
   - Configure system-wide models and tools
   - Manage all workspaces
   - Grant permissions to other users
   - Has Admin role in every workspace automatically

## Security Considerations

1. **Least Privilege**: Users start with minimal permissions
2. **Explicit Grants**: All permissions must be explicitly granted
3. **No Self-Elevation**: Users cannot grant themselves permissions
4. **Audit Trail**: All permission changes logged
5. **Workspace Isolation**: Complete data isolation between workspaces
6. **Role Immutability**: Roles cannot be modified while active
7. **Session Management**: Role changes require re-authentication