/**
 * Unit tests for RoleManagement component.
 * 
 * Tests the functionality of the role management interface including
 * role CRUD operations, permission management, and RBAC features.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { ThemeProvider } from '@mui/material/styles';
import { BrowserRouter } from 'react-router-dom';
import configureStore from 'redux-mock-store';
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';

import RoleManagement from '../RoleManagement';
import * as RoleService from '../../../api/RoleService';
import theme from '../../../theme';

// Mock the RoleService
vi.mock('../../../api/RoleService');

const mockStore = configureStore([]);

const renderWithProviders = (component: React.ReactElement, initialState = {}) => {
  const store = mockStore({
    auth: {
      user: {
        id: '123',
        username: 'testuser',
        roles: ['admin'],
        is_superuser: true
      }
    },
    ...initialState
  });

  return render(
    <Provider store={store}>
      <BrowserRouter>
        <ThemeProvider theme={theme}>
          {component}
        </ThemeProvider>
      </BrowserRouter>
    </Provider>
  );
};

const mockRoles = [
  {
    id: '1',
    name: 'admin',
    description: 'Administrator role with full access',
    permissions: ['read', 'write', 'delete', 'admin'],
    is_active: true,
    is_system_role: true,
    user_count: 5,
    created_at: '2024-01-01T00:00:00Z'
  },
  {
    id: '2',
    name: 'editor',
    description: 'Editor role with read and write access',
    permissions: ['read', 'write'],
    is_active: true,
    is_system_role: false,
    user_count: 12,
    created_at: '2024-01-02T00:00:00Z'
  },
  {
    id: '3',
    name: 'viewer',
    description: 'Viewer role with read-only access',
    permissions: ['read'],
    is_active: true,
    is_system_role: false,
    user_count: 25,
    created_at: '2024-01-03T00:00:00Z'
  }
];

const mockPermissions = [
  { id: 'read', name: 'Read', description: 'View data and resources' },
  { id: 'write', name: 'Write', description: 'Create and modify data' },
  { id: 'delete', name: 'Delete', description: 'Remove data and resources' },
  { id: 'admin', name: 'Admin', description: 'Full administrative access' },
  { id: 'execute', name: 'Execute', description: 'Run workflows and tasks' }
];

describe('RoleManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (RoleService.getRoles as Mock).mockResolvedValue(mockRoles);
    (RoleService.getPermissions as Mock).mockResolvedValue(mockPermissions);
  });

  it('renders role management interface correctly', async () => {
    renderWithProviders(<RoleManagement />);

    expect(screen.getByText('Role Management')).toBeInTheDocument();
    expect(screen.getByText('Create Role')).toBeInTheDocument();
    
    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument();
      expect(screen.getByText('editor')).toBeInTheDocument();
      expect(screen.getByText('viewer')).toBeInTheDocument();
    });
  });

  it('displays roles in table format with correct columns', async () => {
    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('Role Name')).toBeInTheDocument();
      expect(screen.getByText('Description')).toBeInTheDocument();
      expect(screen.getByText('Permissions')).toBeInTheDocument();
      expect(screen.getByText('Users')).toBeInTheDocument();
      expect(screen.getByText('Status')).toBeInTheDocument();
      expect(screen.getByText('Actions')).toBeInTheDocument();
    });
  });

  it('shows system role badge for system roles', async () => {
    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('System Role')).toBeInTheDocument();
    });
  });

  it('opens create role dialog when create button is clicked', async () => {
    renderWithProviders(<RoleManagement />);

    const createButton = screen.getByText('Create Role');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('Create New Role')).toBeInTheDocument();
      expect(screen.getByLabelText('Role Name')).toBeInTheDocument();
      expect(screen.getByLabelText('Description')).toBeInTheDocument();
      expect(screen.getByText('Permissions')).toBeInTheDocument();
    });
  });

  it('creates a new role successfully', async () => {
    const mockCreateRole = vi.fn().mockResolvedValue({
      id: '4',
      name: 'manager',
      description: 'Manager role with limited admin access',
      permissions: ['read', 'write', 'execute'],
      is_active: true
    });
    (RoleService.createRole as Mock).mockImplementation(mockCreateRole);

    renderWithProviders(<RoleManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create Role'));

    await waitFor(() => {
      expect(screen.getByText('Create New Role')).toBeInTheDocument();
    });

    // Fill form
    const nameInput = screen.getByLabelText('Role Name');
    const descriptionInput = screen.getByLabelText('Description');
    
    fireEvent.change(nameInput, { target: { value: 'manager' } });
    fireEvent.change(descriptionInput, { target: { value: 'Manager role with limited admin access' } });

    // Select permissions
    const readPermissionCheckbox = screen.getByLabelText('Read');
    const writePermissionCheckbox = screen.getByLabelText('Write');
    const executePermissionCheckbox = screen.getByLabelText('Execute');
    
    fireEvent.click(readPermissionCheckbox);
    fireEvent.click(writePermissionCheckbox);
    fireEvent.click(executePermissionCheckbox);

    // Submit form
    const submitButton = screen.getByText('Create');
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(mockCreateRole).toHaveBeenCalledWith({
        name: 'manager',
        description: 'Manager role with limited admin access',
        permissions: ['read', 'write', 'execute'],
        is_active: true
      });
    });
  });

  it('opens edit role dialog when edit button is clicked', async () => {
    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('editor')).toBeInTheDocument();
    });

    // Find and click edit button for editor role
    const editButtons = screen.getAllByLabelText('Edit role');
    fireEvent.click(editButtons[1]); // Second role (editor)

    await waitFor(() => {
      expect(screen.getByText('Edit Role')).toBeInTheDocument();
      expect(screen.getByDisplayValue('editor')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Editor role with read and write access')).toBeInTheDocument();
    });
  });

  it('updates role successfully', async () => {
    const mockUpdateRole = vi.fn().mockResolvedValue({
      ...mockRoles[1],
      description: 'Updated editor description'
    });
    (RoleService.updateRole as Mock).mockImplementation(mockUpdateRole);

    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('editor')).toBeInTheDocument();
    });

    // Open edit dialog
    const editButtons = screen.getAllByLabelText('Edit role');
    fireEvent.click(editButtons[1]);

    await waitFor(() => {
      expect(screen.getByText('Edit Role')).toBeInTheDocument();
    });

    // Update description
    const descriptionInput = screen.getByLabelText('Description');
    fireEvent.change(descriptionInput, { target: { value: 'Updated editor description' } });

    // Submit form
    const saveButton = screen.getByText('Save');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockUpdateRole).toHaveBeenCalledWith('2', {
        description: 'Updated editor description',
        permissions: ['read', 'write']
      });
    });
  });

  it('prevents deletion of system roles', async () => {
    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument();
    });

    // System role (admin) should not have delete button enabled
    const deleteButtons = screen.getAllByLabelText('Delete role');
    expect(deleteButtons[0]).toBeDisabled();
  });

  it('deletes non-system role with confirmation', async () => {
    const mockDeleteRole = vi.fn().mockResolvedValue(true);
    (RoleService.deleteRole as Mock).mockImplementation(mockDeleteRole);

    // Mock window.confirm
    window.confirm = vi.fn().mockReturnValue(true);

    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('editor')).toBeInTheDocument();
    });

    // Find and click delete button for non-system role
    const deleteButtons = screen.getAllByLabelText('Delete role');
    fireEvent.click(deleteButtons[1]); // Editor role

    await waitFor(() => {
      expect(window.confirm).toHaveBeenCalledWith('Are you sure you want to delete this role?');
      expect(mockDeleteRole).toHaveBeenCalledWith('2');
    });
  });

  it('opens permission management dialog when manage permissions is clicked', async () => {
    const mockGetRolePermissions = vi.fn().mockResolvedValue(['read', 'write']);
    (RoleService.getRolePermissions as Mock).mockImplementation(mockGetRolePermissions);

    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('editor')).toBeInTheDocument();
    });

    // Find and click manage permissions button
    const managePermissionsButtons = screen.getAllByLabelText('Manage permissions');
    fireEvent.click(managePermissionsButtons[1]);

    await waitFor(() => {
      expect(screen.getByText('Manage Role Permissions')).toBeInTheDocument();
      expect(mockGetRolePermissions).toHaveBeenCalledWith('2');
    });
  });

  it('updates role permissions successfully', async () => {
    const mockGetRolePermissions = vi.fn().mockResolvedValue(['read', 'write']);
    const mockUpdateRolePermissions = vi.fn().mockResolvedValue(true);
    (RoleService.getRolePermissions as Mock).mockImplementation(mockGetRolePermissions);
    (RoleService.updateRolePermissions as Mock).mockImplementation(mockUpdateRolePermissions);

    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('editor')).toBeInTheDocument();
    });

    // Open permission management dialog
    const managePermissionsButtons = screen.getAllByLabelText('Manage permissions');
    fireEvent.click(managePermissionsButtons[1]);

    await waitFor(() => {
      expect(screen.getByText('Manage Role Permissions')).toBeInTheDocument();
    });

    // Add execute permission
    const executePermissionCheckbox = screen.getByLabelText('Execute');
    fireEvent.click(executePermissionCheckbox);

    // Save permissions
    const saveButton = screen.getByText('Save Permissions');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockUpdateRolePermissions).toHaveBeenCalledWith('2', ['read', 'write', 'execute']);
    });
  });

  it('handles search functionality', async () => {
    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument();
      expect(screen.getByText('editor')).toBeInTheDocument();
      expect(screen.getByText('viewer')).toBeInTheDocument();
    });

    // Find search input
    const searchInput = screen.getByPlaceholderText('Search roles...');
    
    // Search for 'edit'
    fireEvent.change(searchInput, { target: { value: 'edit' } });

    await waitFor(() => {
      expect(screen.getByText('editor')).toBeInTheDocument();
      expect(screen.queryByText('admin')).not.toBeInTheDocument();
      expect(screen.queryByText('viewer')).not.toBeInTheDocument();
    });
  });

  it('filters roles by permission', async () => {
    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument();
      expect(screen.getByText('editor')).toBeInTheDocument();
      expect(screen.getByText('viewer')).toBeInTheDocument();
    });

    // Find permission filter
    const permissionFilter = screen.getByLabelText('Filter by Permission');
    fireEvent.mouseDown(permissionFilter);

    await waitFor(() => {
      expect(screen.getByText('Admin')).toBeInTheDocument();
    });

    // Select admin permission
    fireEvent.click(screen.getByText('Admin'));

    await waitFor(() => {
      // Only admin role should be visible
      expect(screen.getByText('admin')).toBeInTheDocument();
      expect(screen.queryByText('editor')).not.toBeInTheDocument();
      expect(screen.queryByText('viewer')).not.toBeInTheDocument();
    });
  });

  it('displays role hierarchy correctly', async () => {
    const mockRoleHierarchy = {
      admin: { level: 0, inherits: [] },
      editor: { level: 1, inherits: ['admin'] },
      viewer: { level: 2, inherits: ['editor'] }
    };
    (RoleService.getRoleHierarchy as Mock).mockResolvedValue(mockRoleHierarchy);

    renderWithProviders(<RoleManagement />);

    // Click hierarchy view button
    const hierarchyButton = screen.getByText('Hierarchy View');
    fireEvent.click(hierarchyButton);

    await waitFor(() => {
      expect(screen.getByText('Role Hierarchy')).toBeInTheDocument();
      expect(RoleService.getRoleHierarchy).toHaveBeenCalled();
    });
  });

  it('validates role name uniqueness', async () => {
    (RoleService.checkRoleNameExists as Mock).mockResolvedValue(true);

    renderWithProviders(<RoleManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create Role'));

    await waitFor(() => {
      expect(screen.getByText('Create New Role')).toBeInTheDocument();
    });

    // Enter existing role name
    const nameInput = screen.getByLabelText('Role Name');
    fireEvent.change(nameInput, { target: { value: 'admin' } });
    fireEvent.blur(nameInput);

    await waitFor(() => {
      expect(screen.getByText('Role name already exists')).toBeInTheDocument();
    });
  });

  it('validates form inputs correctly', async () => {
    renderWithProviders(<RoleManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create Role'));

    await waitFor(() => {
      expect(screen.getByText('Create New Role')).toBeInTheDocument();
    });

    // Try to submit empty form
    const submitButton = screen.getByText('Create');
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(screen.getByText('Role name is required')).toBeInTheDocument();
      expect(screen.getByText('At least one permission must be selected')).toBeInTheDocument();
    });
  });

  it('handles loading state correctly', async () => {
    // Delay the API response to test loading state
    (RoleService.getRoles as Mock).mockImplementation(
      () => new Promise(resolve => setTimeout(() => resolve(mockRoles), 1000))
    );

    renderWithProviders(<RoleManagement />);

    // Should show loading indicator
    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument();
    }, { timeout: 2000 });
  });

  it('handles error state correctly', async () => {
    (RoleService.getRoles as Mock).mockRejectedValue(new Error('Failed to load roles'));

    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('Error loading roles')).toBeInTheDocument();
    });
  });

  it('shows empty state when no roles exist', async () => {
    (RoleService.getRoles as Mock).mockResolvedValue([]);

    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('No roles found')).toBeInTheDocument();
      expect(screen.getByText('Create your first role to get started')).toBeInTheDocument();
    });
  });

  it('handles permissions correctly for non-admin users', async () => {
    const nonAdminState = {
      auth: {
        user: {
          id: '123',
          username: 'testuser',
          roles: ['editor'],
          is_superuser: false
        }
      }
    };

    renderWithProviders(<RoleManagement />, nonAdminState);

    await waitFor(() => {
      // Should not show create button for non-admin users
      expect(screen.queryByText('Create Role')).not.toBeInTheDocument();
      
      // Should not show edit/delete buttons
      expect(screen.queryByLabelText('Edit role')).not.toBeInTheDocument();
      expect(screen.queryByLabelText('Delete role')).not.toBeInTheDocument();
    });
  });

  it('displays permission descriptions in tooltip', async () => {
    renderWithProviders(<RoleManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create Role'));

    await waitFor(() => {
      expect(screen.getByText('Create New Role')).toBeInTheDocument();
    });

    // Hover over a permission to see tooltip
    const readPermissionLabel = screen.getByLabelText('Read');
    fireEvent.mouseEnter(readPermissionLabel);

    await waitFor(() => {
      expect(screen.getByText('View data and resources')).toBeInTheDocument();
    });
  });

  it('handles role cloning functionality', async () => {
    const mockCloneRole = vi.fn().mockResolvedValue({
      id: '5',
      name: 'editor_copy',
      description: 'Copy of Editor role with read and write access',
      permissions: ['read', 'write'],
      is_active: true
    });
    (RoleService.cloneRole as Mock).mockImplementation(mockCloneRole);

    renderWithProviders(<RoleManagement />);

    await waitFor(() => {
      expect(screen.getByText('editor')).toBeInTheDocument();
    });

    // Find and click clone button
    const cloneButtons = screen.getAllByLabelText('Clone role');
    fireEvent.click(cloneButtons[1]);

    await waitFor(() => {
      expect(screen.getByText('Clone Role')).toBeInTheDocument();
    });

    // Enter new name for cloned role
    const nameInput = screen.getByLabelText('New Role Name');
    fireEvent.change(nameInput, { target: { value: 'editor_copy' } });

    // Submit clone
    const cloneButton = screen.getByText('Clone');
    fireEvent.click(cloneButton);

    await waitFor(() => {
      expect(mockCloneRole).toHaveBeenCalledWith('2', 'editor_copy');
    });
  });
});