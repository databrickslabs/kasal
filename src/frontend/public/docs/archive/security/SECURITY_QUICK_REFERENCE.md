# Kasal Security - Quick Reference Card

## 🔐 Authentication
- **Auto-login** via Databricks/OAuth headers
- **No passwords** - external authentication
- **Users created automatically** on first access

## 🎯 Two User-Level Permissions

| Permission | What It Means | How to Get It |
|------------|---------------|---------------|
| **System Admin** | Manage entire system, all workspaces | Granted by existing System Admin |
| **Personal Workspace Manager** | Configure personal workspace | Granted by System Admin |

## 👥 Three Workspace Roles

| Role | Icon | Can Do | Cannot Do |
|------|------|--------|-----------|
| **Admin** | 👑 | Configure workspace, manage members, all operations | Access other workspaces |
| **Editor** | ✏️ | Create/edit workflows, execute, manage API keys | Configure workspace settings |
| **Operator** | 🎮 | Execute workflows, monitor, view logs | Create, edit, or configure |

## 🏠 Workspace Types

### Personal Workspace
- **Format**: `user_[email]`
- **Default Role**: Editor (can work but not configure)
- **With Permission**: Admin (if Personal Workspace Manager)
- **Privacy**: Complete isolation

### Team Workspaces
- **Purpose**: Shared collaboration
- **Roles**: Assigned per user (Admin/Editor/Operator)
- **Management**: Workspace Admin manages
- **Isolation**: Complete from other workspaces

## 🔑 Permission Logic

```python
# How your role is determined:
if is_system_admin:
    return "Admin"  # Everywhere

if personal_workspace:
    if is_personal_workspace_manager:
        return "Admin"  # Can configure
    else:
        return "Editor"  # Can work, not configure

if team_workspace:
    return assigned_role  # Admin/Editor/Operator
```

## ✅ Quick Permission Checks

**"Can I configure my personal workspace?"**
- ✅ If you have Personal Workspace Manager permission
- ❌ Regular users (Editor role only)

**"Can I create workflows?"**
- ✅ Admin or Editor role
- ❌ Operator role

**"Can I configure Databricks/Memory?"**
- ✅ Workspace Admin only
- ❌ Editor, Operator

**"Can I manage team members?"**
- ✅ Workspace Admin (team workspaces)
- ❌ Editor, Operator
- N/A Personal workspaces (no members)

**"Can I access other workspaces?"**
- ✅ System Admin (all workspaces)
- ❌ Everyone else (complete isolation)

## 🛡️ Code Implementation

```python
# Backend
from core.permissions import is_workspace_admin

if is_workspace_admin(group_context):
    # Can configure workspace
```

```typescript
// Frontend
const isWorkspaceAdmin = userRole === 'admin';
const canConfigure = isWorkspaceAdmin || isPersonalWorkspaceManager;
```

## 📁 Key Files
- **User permissions**: Database `users` table
- **Workspace roles**: Database `group_users` table
- **Backend logic**: `/src/backend/src/core/permissions.py`
- **Frontend store**: `/src/frontend/src/store/permissions.ts`