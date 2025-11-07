# Zustand-Based Permission System Implementation

## Overview
This permission system leverages Zustand for efficient state management, providing a centralized, performant solution for role-based access control in the UI.

## Architecture

### Core Components

1. **Permission Store** (`store/permissions.ts`)
   - Zustand store with persist middleware
   - Centralized permission state
   - Computed permissions and helpers
   - Automatic synchronization with backend

2. **Permission Hooks** (`hooks/usePermissions.ts`)
   - `usePermissionLoader`: Auto-loads permissions on group change
   - `usePermissions`: General permission access
   - `useNavigationPermissions`: Menu visibility
   - `useEditPermissions`: Form/edit state

3. **Permission Components** (`components/Common/PermissionGuard.tsx`)
   - `PermissionGuard`: Conditional rendering/disabling
   - `PermissionBoundary`: Section protection
   - `RoleGuard`: Role-based rendering

## Implementation Guide

### 1. App-Level Setup

```tsx
// src/app/index.tsx
import React, { useEffect } from 'react';
import { usePermissionLoader } from '../hooks/usePermissions';
import { usePermissionStore } from '../store/permissions';

function App() {
  // Auto-load permissions on mount and group changes
  usePermissionLoader();

  // Optional: Show loading state while permissions load
  const isLoading = usePermissionStore(state => state.isLoading);

  if (isLoading) {
    return <LoadingSpinner />;
  }

  return (
    <Router>
      <Routes>
        {/* Your app routes */}
      </Routes>
    </Router>
  );
}
```

### 2. Navigation Menu with Permissions

```tsx
// src/components/Navigation/AppNavigation.tsx
import { useNavigationPermissions } from '../../hooks/usePermissions';
import { PermissionGuard, Permissions } from '../Common/PermissionGuard';

function AppNavigation() {
  const { visibleMenuItems, showAdminMenu } = useNavigationPermissions();

  return (
    <Drawer>
      <List>
        {/* Always visible items */}
        <ListItem button component={Link} to="/dashboard">
          <ListItemIcon><DashboardIcon /></ListItemIcon>
          <ListItemText primary="Dashboard" />
        </ListItem>

        {/* Conditionally visible based on permissions */}
        {visibleMenuItems.includes('crews') && (
          <ListItem button component={Link} to="/crews">
            <ListItemIcon><GroupWorkIcon /></ListItemIcon>
            <ListItemText primary="Crews" />
          </ListItem>
        )}

        {/* Admin-only section */}
        {showAdminMenu && (
          <>
            <Divider />
            <ListSubheader>Administration</ListSubheader>
            <ListItem button component={Link} to="/users">
              <ListItemIcon><PeopleIcon /></ListItemIcon>
              <ListItemText primary="User Management" />
            </ListItem>
            <ListItem button component={Link} to="/settings">
              <ListItemIcon><SettingsIcon /></ListItemIcon>
              <ListItemText primary="System Settings" />
            </ListItem>
          </>
        )}
      </List>
    </Drawer>
  );
}
```

### 3. Crew Management with Permissions

```tsx
// src/components/Crews/CrewList.tsx
import { usePermissions } from '../../hooks/usePermissions';
import { PermissionGuard, Permissions } from '../Common/PermissionGuard';
import { usePermissionStore } from '../../store/permissions';

function CrewList() {
  const { canCreate, canEdit, canDelete, canExecute } = usePermissions();
  const hasDeletePermission = usePermissionStore(state =>
    state.hasPermission(Permissions.CREW_DELETE)
  );

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h4">Crews</Typography>

        {/* Only show create button if user can create */}
        <PermissionGuard permission={Permissions.CREW_CREATE}>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={handleCreate}
          >
            Create Crew
          </Button>
        </PermissionGuard>
      </Box>

      <TableContainer>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {crews.map((crew) => (
              <TableRow key={crew.id}>
                <TableCell>{crew.name}</TableCell>
                <TableCell>{crew.status}</TableCell>
                <TableCell>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    {/* Execute button - visible to all who can execute */}
                    <PermissionGuard
                      permission={Permissions.CREW_EXECUTE}
                      disableOnly
                      tooltip="You need execution permission to run this crew"
                    >
                      <IconButton onClick={() => handleExecute(crew.id)}>
                        <PlayArrowIcon />
                      </IconButton>
                    </PermissionGuard>

                    {/* Edit button - only for editors/admins */}
                    <PermissionGuard permission={Permissions.CREW_UPDATE}>
                      <IconButton onClick={() => handleEdit(crew.id)}>
                        <EditIcon />
                      </IconButton>
                    </PermissionGuard>

                    {/* Delete button - only for those with delete permission */}
                    {hasDeletePermission && (
                      <IconButton
                        onClick={() => handleDelete(crew.id)}
                        color="error"
                      >
                        <DeleteIcon />
                      </IconButton>
                    )}
                  </Box>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
```

### 4. Form with Edit Permissions

```tsx
// src/components/Tasks/TaskForm.tsx
import { useEditPermissions } from '../../hooks/usePermissions';
import { RoleGuard } from '../Common/PermissionGuard';

function TaskForm({ task }: { task?: Task }) {
  const { canEdit, isReadOnly } = useEditPermissions();
  const isEditing = !!task;

  return (
    <form>
      <TextField
        label="Task Name"
        value={task?.name || ''}
        disabled={isReadOnly || (isEditing && !canEdit)}
        helperText={isReadOnly ? "Read-only mode" : ""}
        fullWidth
        margin="normal"
      />

      <TextField
        label="Description"
        value={task?.description || ''}
        disabled={isReadOnly || (isEditing && !canEdit)}
        multiline
        rows={4}
        fullWidth
        margin="normal"
      />

      {/* Advanced settings only for editors and admins */}
      <RoleGuard allowedRoles={['admin', 'editor']}>
        <Box sx={{ mt: 2, p: 2, border: '1px solid', borderColor: 'divider' }}>
          <Typography variant="h6" gutterBottom>
            Advanced Settings
          </Typography>
          <FormControlLabel
            control={<Switch />}
            label="Enable delegation"
            disabled={isReadOnly}
          />
          <FormControlLabel
            control={<Switch />}
            label="Allow parallel execution"
            disabled={isReadOnly}
          />
        </Box>
      </RoleGuard>

      <Box sx={{ mt: 2, display: 'flex', gap: 2 }}>
        <Button
          variant="contained"
          disabled={isReadOnly || (isEditing && !canEdit)}
          type="submit"
        >
          {isEditing ? 'Update' : 'Create'}
        </Button>

        <RoleGuard allowedRoles={['admin']}>
          <Button
            variant="outlined"
            color="error"
            onClick={handleDelete}
          >
            Delete
          </Button>
        </RoleGuard>
      </Box>
    </form>
  );
}
```

### 5. Protected Page/Section

```tsx
// src/pages/Settings.tsx
import { PermissionBoundary, Permissions } from '../components/Common/PermissionGuard';

function SettingsPage() {
  return (
    <PermissionBoundary
      permission={[Permissions.SETTINGS_UPDATE, Permissions.MODEL_CONFIGURE]}
      message="You need admin or editor permissions to access settings"
    >
      <SettingsContent />
    </PermissionBoundary>
  );
}
```

### 6. Dynamic Tabs Based on Permissions

```tsx
// src/components/Configuration/ConfigTabs.tsx
import { usePermissionStore } from '../../store/permissions';
import { Permissions } from '../Common/PermissionGuard';

function ConfigurationTabs() {
  const hasPermission = usePermissionStore(state => state.hasPermission);
  const userRole = usePermissionStore(state => state.userRole);

  const tabs = [
    { label: 'General', value: 'general', permission: null }, // Always visible
    {
      label: 'Models',
      value: 'models',
      permission: Permissions.MODEL_READ
    },
    {
      label: 'API Keys',
      value: 'api-keys',
      permission: Permissions.API_KEY_READ
    },
    {
      label: 'Users',
      value: 'users',
      permission: Permissions.GROUP_MANAGE_USERS,
      roleRequired: 'admin'
    },
  ];

  const visibleTabs = tabs.filter(tab => {
    if (tab.roleRequired && userRole !== tab.roleRequired) return false;
    if (tab.permission && !hasPermission(tab.permission)) return false;
    return true;
  });

  return (
    <Tabs value={activeTab} onChange={handleChange}>
      {visibleTabs.map(tab => (
        <Tab key={tab.value} label={tab.label} value={tab.value} />
      ))}
    </Tabs>
  );
}
```

## Zustand Store Benefits

### 1. Performance
- **Selective Subscriptions**: Components only re-render when specific state changes
- **Optimized Updates**: Fine-grained control over what triggers re-renders
- **Persistent State**: Permissions cached between sessions

### 2. Developer Experience
- **Simple API**: No providers, just hooks
- **TypeScript Support**: Full type safety
- **DevTools Integration**: Built-in debugging support
- **Minimal Boilerplate**: Less code than Context API

### 3. Features
- **Middleware Support**: Persist, devtools, logger, etc.
- **Computed State**: Derived values calculated once
- **Async Actions**: Built-in support for async operations
- **State Persistence**: Automatic localStorage sync

## Testing

### Unit Testing with Zustand

```tsx
import { renderHook, act } from '@testing-library/react-hooks';
import { usePermissionStore } from '../../store/permissions';

describe('Permission Store', () => {
  beforeEach(() => {
    // Reset store state
    usePermissionStore.setState({
      userRole: null,
      permissions: new Set(),
      isLoading: false,
      error: null,
    });
  });

  it('should load admin permissions', () => {
    const { result } = renderHook(() => usePermissionStore());

    act(() => {
      result.current.setUserRole('admin');
    });

    expect(result.current.userRole).toBe('admin');
    expect(result.current.canConfigureSystem()).toBe(true);
    expect(result.current.isAdmin()).toBe(true);
  });

  it('should restrict operator permissions', () => {
    const { result } = renderHook(() => usePermissionStore());

    act(() => {
      result.current.setUserRole('operator');
    });

    expect(result.current.canCreate()).toBe(false);
    expect(result.current.canExecute()).toBe(true);
    expect(result.current.isOperator()).toBe(true);
  });
});
```

### Component Testing

```tsx
import { render, screen } from '@testing-library/react';
import { PermissionGuard } from '../components/Common/PermissionGuard';
import { usePermissionStore } from '../store/permissions';

describe('PermissionGuard', () => {
  it('should hide content for operators', () => {
    // Set operator role
    usePermissionStore.setState({
      userRole: 'operator',
      permissions: new Set(['task:read', 'task:execute']),
    });

    render(
      <PermissionGuard permission="task:create">
        <button>Create Task</button>
      </PermissionGuard>
    );

    expect(screen.queryByText('Create Task')).not.toBeInTheDocument();
  });

  it('should show content for editors', () => {
    // Set editor role
    usePermissionStore.setState({
      userRole: 'editor',
      permissions: new Set(['task:create', 'task:update']),
    });

    render(
      <PermissionGuard permission="task:create">
        <button>Create Task</button>
      </PermissionGuard>
    );

    expect(screen.getByText('Create Task')).toBeInTheDocument();
  });
});
```

## Migration from Context to Zustand

### Before (Context API)
```tsx
// Old way with Context
const PermissionContext = createContext();

export const PermissionProvider = ({ children }) => {
  const [permissions, setPermissions] = useState();
  // ... lots of boilerplate
  return (
    <PermissionContext.Provider value={...}>
      {children}
    </PermissionContext.Provider>
  );
};

// Component usage
const Component = () => {
  const { hasPermission } = useContext(PermissionContext);
  // ...
};
```

### After (Zustand)
```tsx
// New way with Zustand
export const usePermissionStore = create((set, get) => ({
  permissions: new Set(),
  hasPermission: (perm) => get().permissions.has(perm),
  // ... clean, simple API
}));

// Component usage - no provider needed!
const Component = () => {
  const hasPermission = usePermissionStore(state => state.hasPermission);
  // ...
};
```

## Best Practices

1. **Use Selectors for Performance**
   ```tsx
   // Good - only re-renders when userRole changes
   const userRole = usePermissionStore(state => state.userRole);

   // Avoid - re-renders on any store change
   const store = usePermissionStore();
   ```

2. **Compute Once, Use Many**
   ```tsx
   // Store computed values in the component
   const { canEdit, canDelete } = usePermissions();
   // Use them multiple times without recomputing
   ```

3. **Group Related Permissions**
   ```tsx
   // Check multiple permissions at once
   <PermissionGuard
     permission={[Permissions.TASK_CREATE, Permissions.TASK_UPDATE]}
     requireAll
   >
   ```

4. **Provide User Feedback**
   ```tsx
   <PermissionGuard
     permission={Permissions.CREW_DELETE}
     disableOnly
     tooltip="Only admins can delete crews"
   >
   ```

## Debugging

### Using Redux DevTools
The Zustand store is configured with devtools middleware:

```tsx
// In browser DevTools
// Redux tab will show:
// - Current state
// - Action history
// - Time travel debugging
```

### Checking Current Permissions
```tsx
// In console
const state = usePermissionStore.getState();
console.log('Current role:', state.userRole);
console.log('Permissions:', Array.from(state.permissions));
console.log('Can edit?', state.canEdit());
```

## Summary

The Zustand-based permission system provides:

1. **Better Performance**: Optimized re-renders with selective subscriptions
2. **Simpler API**: No providers, just hooks
3. **Persistence**: Automatic state persistence
4. **Type Safety**: Full TypeScript support
5. **Developer Experience**: Less boilerplate, better debugging
6. **Scalability**: Easy to extend and maintain

The system ensures proper role-based access control while maintaining excellent performance and developer experience.