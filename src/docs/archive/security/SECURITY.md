# Kasal Security Model

## Overview

Kasal uses a **teamspace-based** security model with **role-based access control (RBAC)**. The system has two types of permissions:
1. **User-level permissions** (properties on the user account)
2. **Teamspace roles** (roles within each teamspace)

## User-Level Permissions

These are flags on the user account that grant system-wide or personal capabilities:

### 1. **System Admin** (`is_system_admin`)
- Manages the entire system
- Configures system-wide settings (models, tools, database)
- Can create/delete any teamspace
- Can grant permissions to other users
- Has Admin role in every teamspace

### 2. **Personal Space Manager** (`is_personal_workspace_manager`)
- Can configure their Personal Space
- Gets Admin role in their Personal Space
- Can set up Databricks, Memory Backend, Volumes for personal use
- Granted by System Admin

## Teamspace Roles

Every teamspace (personal or shared) has three roles:

### 1. **Admin** 👑
- Full control of the teamspace
- Configure teamspace settings (Databricks, Memory, Volumes)
- Manage teamspace members (shared teamspaces only)
- Create, edit, and delete all resources

### 2. **Editor** ✏️
- Create and modify workflows (agents, tasks, crews)
- Execute workflows
- View execution history
- Manage API keys
- Cannot configure teamspace settings

### 3. **Operator** 🎮
- Execute existing workflows
- Monitor execution status
- View logs and results
- Cannot create or modify anything

## How Roles Work

### Personal Spaces
Every user automatically gets a Personal Space (`user_[email]`):
- **With Personal Space Manager permission**: User has **Admin** role
- **Without permission**: User has **Editor** role (can work but not configure)

### Shared Teamspaces
Created for collaboration:
- Users are assigned one of the three roles (Admin/Editor/Operator)
- The Teamspace Admin manages settings and members
- Complete data isolation between teamspaces

## Permission Resolution

```python
# How the system determines your role in a teamspace:

if user.is_system_admin:
    return "Admin"  # System admins are admin everywhere

if teamspace.is_personal:
    if user.is_personal_workspace_manager:
        return "Admin"  # Can configure their Personal Space
    else:
        return "Editor"  # Can work but not configure

if teamspace.is_shared:
    return teamspace.get_user_role(user)  # Admin/Editor/Operator as assigned
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

## Example Scenarios

### Alice - Regular User
```yaml
is_system_admin: false
is_personal_workspace_manager: false

Personal Space:
  Role: Editor
  Can: Create and run workflows
  Cannot: Configure Databricks or Memory

Shared Teamspace "DevTeam":
  Role: Editor (assigned)
  Can: Create and run workflows
  Cannot: Configure the teamspace or manage members
```

### Bob - Personal Space Manager
```yaml
is_system_admin: false
is_personal_workspace_manager: true

Personal Space:
  Role: Admin
  Can: Configure everything, create workflows

Shared Teamspace "DevTeam":
  Role: Operator (assigned)
  Can: Only execute workflows
  Cannot: Create or configure anything
```

### Charlie - Shared Teamspace Admin
```yaml
is_system_admin: false
is_personal_workspace_manager: false

Personal Space:
  Role: Editor
  Cannot: Configure their Personal Space

Shared Teamspace "DevTeam":
  Role: Admin (assigned)
  Can: Configure the shared teamspace, manage members
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

- Each teamspace has complete data isolation
- All database records include `group_id` field
- API endpoints filter by teamspace context
- Users cannot access data from other teamspaces

## Authentication Flow

1. User logs in via OAuth/JWT
2. System loads user permissions (`is_system_admin`, `is_personal_workspace_manager`)
3. User selects active teamspace
4. System determines effective role based on teamspace and permissions
5. All operations scoped to teamspace + role

## Common Questions

**Q: I can't configure Databricks in my Personal Space**
> You need Personal Space Manager permission. Contact your System Admin.

**Q: I'm Admin in TeamA but can't configure my Personal Space**
> Teamspace roles are separate. You need Personal Space Manager permission for Personal Space configuration.

**Q: How do I become a System Admin?**
> Only existing System Admins can grant this permission.

**Q: Why can't I see settings in my Personal Space?**
> By default, users are Editors in their Personal Space. A System Admin must grant you Personal Space Manager permission.

## Security Best Practices

1. **Least Privilege**: Users start with minimal permissions
2. **Explicit Grants**: Configuration rights must be explicitly granted
3. **Teamspace Isolation**: No cross-teamspace data access
4. **Audit Trail**: All actions logged with user + teamspace context
5. **Central Control**: System Admins manage who can configure infrastructure
