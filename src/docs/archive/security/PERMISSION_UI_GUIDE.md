# UI Permission System Implementation Guide

## Overview
The UI permission system provides role-based feature hiding/disabling to ensure users only see and interact with features they have permission to use.

## Architecture

### Core Components

1. **usePermissions Hook** (`hooks/usePermissions.tsx`)
   - Provides permission checking functionality
   - Manages user role and permission state
   - Offers helper methods for common permission checks

2. **PermissionGuard Component** (`components/Common/PermissionGuard.tsx`)
   - Conditionally renders or disables components
   - Supports multiple display modes (hide, disable, lock)
   - Provides tooltip explanations

3. **PermissionBoundary Component**
   - Protects entire sections/pages
   - Shows access denied message for unauthorized users

## Implementation Examples

### 1. Hiding Buttons Based on Permissions

```tsx
import { PermissionGuard, Permissions } from '../Common/PermissionGuard';

function CrewList() {
  return (
    <div>
      {/* Only admins and editors can create crews */}
      <PermissionGuard permission={Permissions.CREW_CREATE}>
        <Button onClick={handleCreate}>
          Create New Crew
        </Button>
      </PermissionGuard>

      {/* Only admins can delete */}
      <PermissionGuard permission={Permissions.CREW_DELETE}>
        <IconButton onClick={handleDelete}>
          <DeleteIcon />
        </IconButton>
      </PermissionGuard>
    </div>
  );
}
```

### 2. Disabling Features (Visible but Disabled)

```tsx
function TaskEditor() {
  return (
    <div>
      {/* Disable save button for operators */}
      <PermissionGuard
        permission={Permissions.TASK_UPDATE}
        disableOnly
        tooltip="Only Editors and Admins can modify tasks"
      >
        <Button variant="contained" onClick={handleSave}>
          Save Changes
        </Button>
      </PermissionGuard>
    </div>
  );
}
```

### 3. Protecting Entire Pages/Sections

```tsx
import { PermissionBoundary, Permissions } from '../Common/PermissionGuard';

function ConfigurationPage() {
  return (
    <PermissionBoundary
      permission={[Permissions.SETTINGS_UPDATE, Permissions.MODEL_CONFIGURE]}
      message="Configuration requires Admin or Editor role"
    >
      <ConfigurationContent />
    </PermissionBoundary>
  );
}
```

### 4. Conditional Menu Items

```tsx
import { usePermissions, Permissions } from '../../hooks/usePermissions';

function NavigationMenu() {
  const { hasPermission, userRole } = usePermissions();

  return (
    <List>
      <ListItem button onClick={() => navigate('/crews')}>
        <ListItemText primary="Crews" />
      </ListItem>

      {hasPermission(Permissions.CREW_CREATE) && (
        <ListItem button onClick={() => navigate('/crews/new')}>
          <ListItemText primary="Create Crew" />
        </ListItem>
      )}

      {hasPermission(Permissions.SETTINGS_UPDATE) && (
        <ListItem button onClick={() => navigate('/settings')}>
          <ListItemText primary="Settings" />
        </ListItem>
      )}

      {userRole === 'admin' && (
        <ListItem button onClick={() => navigate('/admin')}>
          <ListItemText primary="Admin Panel" />
        </ListItem>
      )}
    </List>
  );
}
```

### 5. Dynamic Form Fields

```tsx
function WorkflowForm() {
  const { canEdit, canDelete, canConfigureSystem } = usePermissions();

  return (
    <form>
      <TextField
        label="Workflow Name"
        disabled={!canEdit}
        helperText={!canEdit ? "Read-only mode" : ""}
      />

      <PermissionGuard permission={Permissions.MODEL_CONFIGURE}>
        <FormControl>
          <InputLabel>Model Selection</InputLabel>
          <Select>
            <MenuItem value="gpt-4">GPT-4</MenuItem>
            <MenuItem value="claude">Claude</MenuItem>
          </Select>
        </FormControl>
      </PermissionGuard>

      <Box sx={{ mt: 2, display: 'flex', gap: 2 }}>
        <Button
          variant="contained"
          disabled={!canEdit}
        >
          Save
        </Button>

        <PermissionGuard permission={Permissions.CREW_DELETE}>
          <Button
            variant="outlined"
            color="error"
            onClick={handleDelete}
          >
            Delete
          </Button>
        </PermissionGuard>
      </Box>
    </form>
  );
}
```

### 6. Table Actions Based on Permissions

```tsx
function ExecutionTable() {
  const { hasPermission } = usePermissions();

  const columns = [
    { field: 'id', headerName: 'ID' },
    { field: 'name', headerName: 'Name' },
    { field: 'status', headerName: 'Status' },
    {
      field: 'actions',
      headerName: 'Actions',
      renderCell: (params) => (
        <Box sx={{ display: 'flex', gap: 1 }}>
          {/* Everyone can view */}
          <IconButton onClick={() => handleView(params.row)}>
            <VisibilityIcon />
          </IconButton>

          {/* Only those with execute permission can run */}
          <PermissionGuard
            permission={Permissions.TASK_EXECUTE}
            disableOnly
            tooltip="Requires execution permission"
          >
            <IconButton onClick={() => handleExecute(params.row)}>
              <PlayArrowIcon />
            </IconButton>
          </PermissionGuard>

          {/* Only those with manage permission can stop */}
          <PermissionGuard permission={Permissions.EXECUTION_MANAGE}>
            <IconButton onClick={() => handleStop(params.row)}>
              <StopIcon />
            </IconButton>
          </PermissionGuard>
        </Box>
      ),
    },
  ];

  return <DataGrid rows={data} columns={columns} />;
}
```

## Integration Steps

### 1. Wrap App with PermissionProvider

In your main App component:

```tsx
// src/app/index.tsx
import { PermissionProvider } from '../hooks/usePermissions';

function App() {
  return (
    <PermissionProvider>
      <Router>
        {/* Your app routes */}
      </Router>
    </PermissionProvider>
  );
}
```

### 2. Update Existing Components

For each component that needs permission-based visibility:

1. Import permission utilities
2. Wrap sensitive UI elements with PermissionGuard
3. Use appropriate display mode (hide, disable, lock)
4. Add helpful tooltips for disabled features

### 3. Update Navigation

Modify navigation menus to hide/show items based on permissions:

```tsx
const menuItems = [
  { label: 'Dashboard', path: '/', permission: null }, // Always visible
  { label: 'Crews', path: '/crews', permission: Permissions.CREW_READ },
  { label: 'Create Crew', path: '/crews/new', permission: Permissions.CREW_CREATE },
  { label: 'Settings', path: '/settings', permission: Permissions.SETTINGS_UPDATE },
];

return menuItems
  .filter(item => !item.permission || hasPermission(item.permission))
  .map(item => <MenuItem key={item.path} {...item} />);
```

## Permission Checking Best Practices

### 1. Granular Permissions
Use specific permissions rather than role checks:
```tsx
// Good
<PermissionGuard permission={Permissions.TASK_DELETE}>

// Avoid
{userRole === 'admin' && <DeleteButton />}
```

### 2. Combine Multiple Permissions
For features requiring multiple permissions:
```tsx
<PermissionGuard permission={[Permissions.TASK_CREATE, Permissions.TASK_UPDATE]}>
  <ComplexFeature />
</PermissionGuard>
```

### 3. Provide User Feedback
Always explain why something is disabled:
```tsx
<PermissionGuard
  permission={Permissions.API_KEY_CREATE}
  disableOnly
  tooltip="Admin permission required to create API keys"
>
  <Button>Create API Key</Button>
</PermissionGuard>
```

### 4. Progressive Disclosure
Show read-only views to users without edit permissions:
```tsx
function TaskView() {
  const { canEdit } = usePermissions();

  return (
    <div>
      <TaskDetails readOnly={!canEdit} />

      <PermissionGuard permission={Permissions.TASK_UPDATE}>
        <EditToolbar />
      </PermissionGuard>
    </div>
  );
}
```

## Testing Permissions

### Manual Testing
1. Login with different role accounts (admin, editor, operator)
2. Verify each role sees appropriate features
3. Check that disabled features show helpful messages
4. Ensure no security bypasses through direct URL access

### Automated Testing
```tsx
describe('PermissionGuard', () => {
  it('hides component when permission denied', () => {
    mockPermissions({ role: 'operator', permissions: [] });
    render(
      <PermissionGuard permission={Permissions.CREW_CREATE}>
        <button>Create</button>
      </PermissionGuard>
    );
    expect(screen.queryByText('Create')).not.toBeInTheDocument();
  });

  it('shows component when permission granted', () => {
    mockPermissions({ role: 'editor', permissions: [Permissions.CREW_CREATE] });
    render(
      <PermissionGuard permission={Permissions.CREW_CREATE}>
        <button>Create</button>
      </PermissionGuard>
    );
    expect(screen.getByText('Create')).toBeInTheDocument();
  });
});
```

## Role-Specific UI Guidelines

### Admin UI
- Full access to all features
- Shows all buttons, menus, and configuration options
- Can access system settings, user management, and monitoring

### Editor UI
- Can create, edit, delete workflows
- Cannot access system settings or user management
- Shows workflow design tools and testing features
- API keys visible but read-only

### Operator UI
- Read-only access to workflows
- Can execute and monitor executions
- Cannot modify any configurations
- Simplified UI with focus on execution controls

## Migration Checklist

When implementing permissions in existing components:

- [ ] Identify all user actions in the component
- [ ] Map actions to required permissions
- [ ] Wrap UI elements with PermissionGuard
- [ ] Add tooltips for disabled features
- [ ] Test with all three roles
- [ ] Update component documentation
- [ ] Add permission-based tests

## Common Patterns

### Pattern 1: CRUD Operations
```tsx
<Box sx={{ display: 'flex', gap: 1 }}>
  <PermissionGuard permission={Permissions.RESOURCE_CREATE}>
    <Button startIcon={<AddIcon />}>Create</Button>
  </PermissionGuard>

  <PermissionGuard permission={Permissions.RESOURCE_UPDATE} disableOnly>
    <Button startIcon={<EditIcon />}>Edit</Button>
  </PermissionGuard>

  <PermissionGuard permission={Permissions.RESOURCE_DELETE}>
    <Button startIcon={<DeleteIcon />} color="error">Delete</Button>
  </PermissionGuard>
</Box>
```

### Pattern 2: Configuration Sections
```tsx
<Tabs>
  <Tab label="General" />
  <PermissionGuard permission={Permissions.SETTINGS_UPDATE}>
    <Tab label="Advanced Settings" />
  </PermissionGuard>
  <PermissionGuard permission={Permissions.API_KEY_MANAGE}>
    <Tab label="API Keys" />
  </PermissionGuard>
</Tabs>
```

### Pattern 3: Action Menus
```tsx
<Menu>
  <MenuItem onClick={handleView}>View Details</MenuItem>
  <PermissionGuard permission={Permissions.TASK_EXECUTE}>
    <MenuItem onClick={handleRun}>Run Task</MenuItem>
  </PermissionGuard>
  <Divider />
  <PermissionGuard permission={Permissions.TASK_DELETE}>
    <MenuItem onClick={handleDelete} sx={{ color: 'error.main' }}>
      Delete
    </MenuItem>
  </PermissionGuard>
</Menu>
```