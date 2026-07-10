# Kasal Security - Quick Reference Card

## 🔐 Authentication
- **Auto-login** via Databricks/OAuth headers
- **No passwords** - external authentication
- **Users created automatically** on first access

## 🎯 Two User-Level Permissions

| Permission | What It Means | How to Get It |
|------------|---------------|---------------|
| **System Admin** | Manage entire system, all teamspaces | Granted by existing System Admin |
| **Personal Space Manager** | Configure their Personal Space | Granted by System Admin |

## 👥 Three Teamspace Roles

| Role | Icon | Can Do | Cannot Do |
|------|------|--------|-----------|
| **Admin** | 👑 | Configure the teamspace, manage members, all operations | Access other teamspaces |
| **Editor** | ✏️ | Create/edit workflows, execute, manage API keys | Configure teamspace settings |
| **Operator** | 🎮 | Execute workflows, monitor, view logs | Create, edit, or configure |

## 🏠 Teamspace Types

### Personal Space
- **Format**: `user_[email]`
- **Default Role**: Editor (can work but not configure)
- **With Permission**: Admin (if Personal Space Manager)
- **Privacy**: Complete isolation

### Shared Teamspaces
- **Purpose**: Shared collaboration
- **Roles**: Assigned per user (Admin/Editor/Operator)
- **Management**: The Teamspace Admin manages
- **Isolation**: Complete from other teamspaces

## 🔑 Permission Logic

```python
# How your role is determined:
if is_system_admin:
    return "Admin"  # Everywhere

if personal_teamspace:
    if is_personal_workspace_manager:
        return "Admin"  # Can configure
    else:
        return "Editor"  # Can work, not configure

if shared_teamspace:
    return assigned_role  # Admin/Editor/Operator
```

## ✅ Quick Permission Checks

**"Can I configure my Personal Space?"**
- ✅ If you have Personal Space Manager permission
- ❌ Regular users (Editor role only)

**"Can I create workflows?"**
- ✅ Admin or Editor role
- ❌ Operator role

**"Can I configure Databricks/Memory?"**
- ✅ Teamspace Admin only
- ❌ Editor, Operator

**"Can I manage team members?"**
- ✅ Teamspace Admin (shared teamspaces)
- ❌ Editor, Operator
- N/A Personal Spaces (no members)

**"Can I access other teamspaces?"**
- ✅ System Admin (all teamspaces)
- ❌ Everyone else (complete isolation)

## 🛡️ Code Implementation

```python
# Backend
from core.permissions import is_workspace_admin

if is_workspace_admin(group_context):
    # Can configure the teamspace
```

```typescript
// Frontend
const isWorkspaceAdmin = userRole === 'admin';
const canConfigure = isWorkspaceAdmin || isPersonalWorkspaceManager;
```

## 📁 Key Files
- **User permissions**: Database `users` table
- **Teamspace roles**: Database `group_users` table
- **Backend logic**: `/src/backend/src/core/permissions.py`
- **Frontend store**: `/src/frontend/src/store/permissions.ts`
