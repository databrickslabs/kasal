# Kasal Security Model

## Overview

Kasal uses a **workspace-based** security model with **role-based access control (RBAC)**. The system has two types of permissions:
1. **User-level permissions** (properties on the user account)
2. **Workspace roles** (roles within each workspace)

## User-Level Permissions

These are flags on the user account that grant system-wide or personal capabilities:

### 1. **System Admin** (`is_system_admin`)
- Manages the entire system
- Configures system-wide settings (models, tools, database)
- Can create/delete any workspace
- Can grant permissions to other users
- Has Admin role in every workspace

### 2. **Personal Workspace Manager** (`is_personal_workspace_manager`)
- Can configure their personal workspace
- Gets Admin role in their personal workspace
- Can set up Databricks, Memory Backend, Volumes for personal use
- Granted by System Admin

## Workspace Roles

Every workspace (personal or team) has three roles:

### 1. **Admin** 👑
- Full control of the workspace
- Configure workspace settings (Databricks, Memory, Volumes)
- Manage workspace members (team workspaces only)
- Create, edit, and delete all resources

### 2. **Editor** ✏️
- Create and modify workflows (agents, tasks, crews)
- Execute workflows
- View execution history
- Manage API keys
- Cannot configure workspace settings

### 3. **Operator** 🎮
- Execute existing workflows
- Monitor execution status
- View logs and results
- Cannot create or modify anything

## How Roles Work

### Personal Workspaces
Every user automatically gets a personal workspace (`user_[email]`):
- **With Personal Workspace Manager permission**: User has **Admin** role
- **Without permission**: User has **Editor** role (can work but not configure)

### Team Workspaces
Created for collaboration:
- Users are assigned one of the three roles (Admin/Editor/Operator)
- Workspace Admin manages settings and members
- Complete data isolation between workspaces

## Permission Resolution

```python
# How the system determines your role in a workspace:

if user.is_system_admin:
    return "Admin"  # System admins are admin everywhere

if workspace.is_personal:
    if user.is_personal_workspace_manager:
        return "Admin"  # Can configure personal workspace
    else:
        return "Editor"  # Can work but not configure

if workspace.is_team:
    return workspace.get_user_role(user)  # Admin/Editor/Operator as assigned
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

## Example Scenarios

### Alice - Regular User
```yaml
is_system_admin: false
is_personal_workspace_manager: false

Personal Workspace:
  Role: Editor
  Can: Create and run workflows
  Cannot: Configure Databricks or Memory

Team Workspace "DevTeam":
  Role: Editor (assigned)
  Can: Create and run workflows
  Cannot: Configure workspace or manage members
```

### Bob - Personal Workspace Manager
```yaml
is_system_admin: false
is_personal_workspace_manager: true

Personal Workspace:
  Role: Admin
  Can: Configure everything, create workflows

Team Workspace "DevTeam":
  Role: Operator (assigned)
  Can: Only execute workflows
  Cannot: Create or configure anything
```

### Charlie - Team Workspace Admin
```yaml
is_system_admin: false
is_personal_workspace_manager: false

Personal Workspace:
  Role: Editor
  Cannot: Configure personal workspace

Team Workspace "DevTeam":
  Role: Admin (assigned)
  Can: Configure team workspace, manage members
```

### Diana - System Admin
```yaml
is_system_admin: true
is_personal_workspace_manager: true (implicit)

Everywhere:
  Role: Admin
  Can: Everything
```

## Data Isolation

- Each workspace has complete data isolation
- All database records include `group_id` field
- API endpoints filter by workspace context
- Users cannot access data from other workspaces

## Authentication Flow

1. User logs in via OAuth/JWT
2. System loads user permissions (`is_system_admin`, `is_personal_workspace_manager`)
3. User selects active workspace
4. System determines effective role based on workspace and permissions
5. All operations scoped to workspace + role

## Common Questions

**Q: I can't configure Databricks in my personal workspace**
> You need Personal Workspace Manager permission. Contact your System Admin.

**Q: I'm Admin in TeamA but can't configure my personal workspace**
> Workspace roles are separate. You need Personal Workspace Manager permission for personal workspace configuration.

**Q: How do I become a System Admin?**
> Only existing System Admins can grant this permission.

**Q: Why can't I see settings in my personal workspace?**
> By default, users are Editors in their personal workspace. System Admin must grant you Personal Workspace Manager permission.

## Security Best Practices

1. **Least Privilege**: Users start with minimal permissions
2. **Explicit Grants**: Configuration rights must be explicitly granted
3. **Workspace Isolation**: No cross-workspace data access
4. **Audit Trail**: All actions logged with user + workspace context
5. **Central Control**: System Admins manage who can configure infrastructure