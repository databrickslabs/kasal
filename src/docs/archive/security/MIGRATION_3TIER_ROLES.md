# Migration Guide: 3-Tier Role System

## Overview
This guide covers the migration from the old 4-tier role system (Admin, Manager, User, Viewer) to the new 3-tier system (Admin, Editor, Operator).

## Role Mapping

| Old Role | New Role | Description |
|----------|----------|-------------|
| Admin | Admin | Full system control (unchanged) |
| Manager | Editor | Workflow developer with create/edit permissions |
| User | Operator | Execution operator with run/monitor permissions |
| Viewer | Operator | Read-only users become operators with execution rights |

## Migration Steps

### 1. Backend Database Migration

Run the migration script to update all existing role data:

```bash
cd src/backend
python src/scripts/migrate_roles_to_3tier.py
```

This script will:
- Update all `group_users` records with new role values
- Update `roles` table with new role definitions
- Remove old role definitions
- Show before/after statistics

### 2. Verify Backend Services

The backend has been updated with:
- **Enum definitions** updated in `src/models/enums.py` and `src/models/group.py`
- **Permission mappings** updated in `src/models/group.py` and `src/models/privileges.py`
- **Schema validation** with automatic migration for legacy values in `src/schemas/group.py`
- **Default values** changed from 'user' to 'operator' in services and schemas

### 3. Frontend Updates

The frontend has been updated with:
- **Type definitions** in `src/api/GroupService.ts`
- **UI Components** updated role icons and descriptions
- **Permission System** with Zustand store for efficient state management

### 4. Apply Frontend Changes

After backend migration, restart the frontend to pick up changes:

```bash
cd src/frontend
npm start
```

## Fixes Applied

### Backend Fixes

1. **Validation Errors Fixed**
   - Added `field_validator` in `GroupUserResponse` to automatically migrate legacy role values
   - This prevents "Input should be 'admin', 'editor' or 'operator'" errors

2. **Database Consistency**
   - Migration script updates all existing records
   - New default role is 'operator' instead of 'user'

3. **Permission Mappings**
   - Editor role has workflow creation/editing permissions
   - Operator role has execution and monitoring permissions
   - Admin role retains all permissions

### Frontend Fixes

1. **Type Safety**
   - All TypeScript interfaces updated with new role types
   - Role type: `'admin' | 'editor' | 'operator'`

2. **UI Components**
   - Role selection dropdowns show only 3 roles
   - Role icons: Admin (AdminIcon), Editor (EditIcon), Operator (PersonIcon)
   - Updated descriptions in all UI components

3. **Permission System**
   - Zustand store for efficient permission management
   - PermissionGuard components for conditional rendering
   - Role-based navigation and feature visibility

## Testing the Migration

### 1. Check Current Roles

Before migration, check existing roles:

```sql
SELECT role, COUNT(*) FROM group_users GROUP BY role;
```

### 2. Run Migration

Execute the migration script as shown above.

### 3. Verify Results

After migration, verify all roles are updated:

```sql
SELECT role, COUNT(*) FROM group_users GROUP BY role;
-- Should only show: admin, editor, operator
```

### 4. Test UI Access

Login with different role accounts and verify:

- **Admin**: Full access to all features
- **Editor**: Can create/edit workflows, but no system settings
- **Operator**: Can execute workflows and monitor, but no editing

## Rollback Plan

If needed, you can rollback to 4-tier system:

1. Run the downgrade migration (note: this is lossy for viewer vs user distinction)
2. Revert code changes via git
3. Restart services

## Troubleshooting

### Issue: Validation errors after migration

**Solution**: The schema validator automatically handles legacy values. If errors persist:
1. Check the migration script ran successfully
2. Verify `GroupUserResponse` has the field_validator
3. Restart backend service

### Issue: Frontend shows wrong roles

**Solution**:
1. Clear browser localStorage
2. Refresh the page to reload permissions
3. Verify backend API returns correct role values

### Issue: Permission denied for existing users

**Solution**:
1. Run the permission store refresh: `usePermissionStore.getState().refreshPermissions()`
2. Check user's role in the database
3. Verify permission mappings in the backend

## Benefits of 3-Tier System

1. **Simplified Role Management**
   - Clearer role boundaries
   - Easier to understand permissions
   - Reduced complexity

2. **Better User Experience**
   - Operators have execution rights (upgrade from viewer)
   - Editors have full workflow control
   - Admins maintain system control

3. **Improved Security**
   - Clear separation of concerns
   - Consistent permission model
   - Easier to audit and maintain

## API Changes

### Endpoints Affected

All group and user management endpoints now expect:
- Role values: `'admin'`, `'editor'`, `'operator'`
- Automatic migration of legacy values in responses
- Validation errors for invalid role values in requests

### Example API Calls

```typescript
// Assign user with new role
POST /api/groups/{groupId}/users
{
  "user_email": "user@example.com",
  "role": "editor"  // or "operator", "admin"
}

// Update user role
PUT /api/groups/{groupId}/users/{userId}
{
  "role": "operator"  // Changed from "user"
}
```

## Summary

The migration to a 3-tier role system:
1. ✅ Simplifies role management
2. ✅ Provides clearer permission boundaries
3. ✅ Maintains backward compatibility through validation
4. ✅ Improves overall system maintainability
5. ✅ Enhances user experience with better role definitions

**Migration Status: COMPLETED on 2025-09-20**
- ✅ All backend code updated to 3-tier system
- ✅ All frontend components updated with new roles
- ✅ Pydantic validators added for backward compatibility
- ✅ Database migration script executed successfully
- ✅ All 5 existing users migrated from 'user' role to 'operator' role
- ✅ Alembic migration file created for future deployments
- ✅ **Complex RBAC cleanup completed**: Removed privilege tables, role mappings, external identities
- ✅ **Simplified architecture**: Decorator-based permissions with group-scoped roles
- ✅ System is fully operational with the simplified 3-tier role structure