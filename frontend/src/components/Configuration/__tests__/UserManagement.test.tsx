/**
 * Unit tests for UserManagement component.
 * 
 * Tests the functionality of the user management interface including
 * user CRUD operations, role assignments, and profile management.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { ThemeProvider } from '@mui/material/styles';
import { BrowserRouter } from 'react-router-dom';
import configureStore from 'redux-mock-store';
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';

import UserManagement from '../UserManagement';
import * as UserService from '../../../api/UserService';
import * as RoleService from '../../../api/RoleService';
import * as GroupService from '../../../api/GroupService';
import theme from '../../../theme';

// Mock the services
vi.mock('../../../api/UserService');
vi.mock('../../../api/RoleService');
vi.mock('../../../api/GroupService');

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

const mockUsers = [
  {
    id: '1',
    username: 'john.doe',
    email: 'john@example.com',
    full_name: 'John Doe',
    is_active: true,
    is_superuser: false,
    roles: ['editor'],
    groups: ['developers'],
    last_login: '2024-01-01T10:00:00Z',
    created_at: '2023-12-01T00:00:00Z'
  },
  {
    id: '2',
    username: 'jane.smith',
    email: 'jane@example.com',
    full_name: 'Jane Smith',
    is_active: true,
    is_superuser: true,
    roles: ['admin'],
    groups: ['admins'],
    last_login: '2024-01-01T09:30:00Z',
    created_at: '2023-11-15T00:00:00Z'
  },
  {
    id: '3',
    username: 'bob.wilson',
    email: 'bob@example.com',
    full_name: 'Bob Wilson',
    is_active: false,
    is_superuser: false,
    roles: ['viewer'],
    groups: [],
    last_login: '2023-12-20T15:00:00Z',
    created_at: '2023-10-01T00:00:00Z'
  }
];

const mockRoles = [
  { id: '1', name: 'admin', description: 'Administrator role' },
  { id: '2', name: 'editor', description: 'Editor role' },
  { id: '3', name: 'viewer', description: 'Viewer role' }
];

const mockGroups = [
  { id: '1', name: 'admins', description: 'Administrators group' },
  { id: '2', name: 'developers', description: 'Developers group' },
  { id: '3', name: 'viewers', description: 'Viewers group' }
];

describe('UserManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (UserService.getUsers as Mock).mockResolvedValue(mockUsers);
    (RoleService.getRoles as Mock).mockResolvedValue(mockRoles);
    (GroupService.getGroups as Mock).mockResolvedValue(mockGroups);
  });

  it('renders user management interface correctly', async () => {
    renderWithProviders(<UserManagement />);

    expect(screen.getByText('User Management')).toBeInTheDocument();
    expect(screen.getByText('Create User')).toBeInTheDocument();
    
    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
      expect(screen.getByText('jane.smith')).toBeInTheDocument();
      expect(screen.getByText('bob.wilson')).toBeInTheDocument();
    });
  });

  it('displays users in table format with correct columns', async () => {
    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('Username')).toBeInTheDocument();
      expect(screen.getByText('Full Name')).toBeInTheDocument();
      expect(screen.getByText('Email')).toBeInTheDocument();
      expect(screen.getByText('Roles')).toBeInTheDocument();
      expect(screen.getByText('Status')).toBeInTheDocument();
      expect(screen.getByText('Last Login')).toBeInTheDocument();
      expect(screen.getByText('Actions')).toBeInTheDocument();
    });
  });

  it('shows user status badges correctly', async () => {
    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('Active')).toBeInTheDocument();
      expect(screen.getByText('Inactive')).toBeInTheDocument();
      expect(screen.getByText('Superuser')).toBeInTheDocument();
    });
  });

  it('opens create user dialog when create button is clicked', async () => {
    renderWithProviders(<UserManagement />);

    const createButton = screen.getByText('Create User');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('Create New User')).toBeInTheDocument();
      expect(screen.getByLabelText('Username')).toBeInTheDocument();
      expect(screen.getByLabelText('Email')).toBeInTheDocument();
      expect(screen.getByLabelText('Full Name')).toBeInTheDocument();
      expect(screen.getByLabelText('Password')).toBeInTheDocument();
    });
  });

  it('creates a new user successfully', async () => {
    const mockCreateUser = vi.fn().mockResolvedValue({
      id: '4',
      username: 'new.user',
      email: 'newuser@example.com',
      full_name: 'New User',
      is_active: true,
      roles: ['viewer']
    });
    (UserService.createUser as Mock).mockImplementation(mockCreateUser);

    renderWithProviders(<UserManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create User'));

    await waitFor(() => {
      expect(screen.getByText('Create New User')).toBeInTheDocument();
    });

    // Fill form
    const usernameInput = screen.getByLabelText('Username');
    const emailInput = screen.getByLabelText('Email');
    const fullNameInput = screen.getByLabelText('Full Name');
    const passwordInput = screen.getByLabelText('Password');
    
    fireEvent.change(usernameInput, { target: { value: 'new.user' } });
    fireEvent.change(emailInput, { target: { value: 'newuser@example.com' } });
    fireEvent.change(fullNameInput, { target: { value: 'New User' } });
    fireEvent.change(passwordInput, { target: { value: 'SecurePassword123!' } });

    // Select role
    const roleSelect = screen.getByLabelText('Roles');
    fireEvent.mouseDown(roleSelect);

    await waitFor(() => {
      expect(screen.getByText('viewer')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('viewer'));

    // Submit form
    const submitButton = screen.getByText('Create');
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(mockCreateUser).toHaveBeenCalledWith({
        username: 'new.user',
        email: 'newuser@example.com',
        full_name: 'New User',
        password: 'SecurePassword123!',
        roles: ['viewer'],
        is_active: true
      });
    });
  });

  it('opens edit user dialog when edit button is clicked', async () => {
    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Find and click edit button for first user
    const editButtons = screen.getAllByLabelText('Edit user');
    fireEvent.click(editButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Edit User')).toBeInTheDocument();
      expect(screen.getByDisplayValue('john.doe')).toBeInTheDocument();
      expect(screen.getByDisplayValue('John Doe')).toBeInTheDocument();
    });
  });

  it('updates user successfully', async () => {
    const mockUpdateUser = vi.fn().mockResolvedValue({
      ...mockUsers[0],
      full_name: 'John Updated Doe'
    });
    (UserService.updateUser as Mock).mockImplementation(mockUpdateUser);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Open edit dialog
    const editButtons = screen.getAllByLabelText('Edit user');
    fireEvent.click(editButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Edit User')).toBeInTheDocument();
    });

    // Update full name
    const fullNameInput = screen.getByLabelText('Full Name');
    fireEvent.change(fullNameInput, { target: { value: 'John Updated Doe' } });

    // Submit form
    const saveButton = screen.getByText('Save');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockUpdateUser).toHaveBeenCalledWith('1', {
        full_name: 'John Updated Doe'
      });
    });
  });

  it('deactivates user with confirmation', async () => {
    const mockDeactivateUser = vi.fn().mockResolvedValue(true);
    (UserService.deactivateUser as Mock).mockImplementation(mockDeactivateUser);

    // Mock window.confirm
    window.confirm = vi.fn().mockReturnValue(true);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Find and click deactivate button
    const deactivateButtons = screen.getAllByLabelText('Deactivate user');
    fireEvent.click(deactivateButtons[0]);

    await waitFor(() => {
      expect(window.confirm).toHaveBeenCalledWith('Are you sure you want to deactivate this user?');
      expect(mockDeactivateUser).toHaveBeenCalledWith('1');
    });
  });

  it('activates inactive user', async () => {
    const mockActivateUser = vi.fn().mockResolvedValue(true);
    (UserService.activateUser as Mock).mockImplementation(mockActivateUser);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('bob.wilson')).toBeInTheDocument();
    });

    // Find and click activate button for inactive user
    const activateButtons = screen.getAllByLabelText('Activate user');
    fireEvent.click(activateButtons[0]);

    await waitFor(() => {
      expect(mockActivateUser).toHaveBeenCalledWith('3');
    });
  });

  it('opens role management dialog when manage roles is clicked', async () => {
    const mockGetUserRoles = vi.fn().mockResolvedValue(['editor']);
    (UserService.getUserRoles as Mock).mockImplementation(mockGetUserRoles);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Find and click manage roles button
    const manageRolesButtons = screen.getAllByLabelText('Manage roles');
    fireEvent.click(manageRolesButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Manage User Roles')).toBeInTheDocument();
      expect(mockGetUserRoles).toHaveBeenCalledWith('1');
    });
  });

  it('assigns role to user successfully', async () => {
    const mockGetUserRoles = vi.fn().mockResolvedValue(['editor']);
    const mockAssignRole = vi.fn().mockResolvedValue(true);
    (UserService.getUserRoles as Mock).mockImplementation(mockGetUserRoles);
    (UserService.assignRoleToUser as Mock).mockImplementation(mockAssignRole);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Open role management dialog
    const manageRolesButtons = screen.getAllByLabelText('Manage roles');
    fireEvent.click(manageRolesButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Manage User Roles')).toBeInTheDocument();
    });

    // Add admin role
    const addRoleButton = screen.getByText('Add Role');
    fireEvent.click(addRoleButton);

    await waitFor(() => {
      expect(screen.getByLabelText('Select Role')).toBeInTheDocument();
    });

    const roleSelect = screen.getByLabelText('Select Role');
    fireEvent.mouseDown(roleSelect);

    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('admin'));

    const assignButton = screen.getByText('Assign');
    fireEvent.click(assignButton);

    await waitFor(() => {
      expect(mockAssignRole).toHaveBeenCalledWith('1', 'admin');
    });
  });

  it('removes role from user successfully', async () => {
    const mockGetUserRoles = vi.fn().mockResolvedValue(['editor', 'admin']);
    const mockRemoveRole = vi.fn().mockResolvedValue(true);
    (UserService.getUserRoles as Mock).mockImplementation(mockGetUserRoles);
    (UserService.removeRoleFromUser as Mock).mockImplementation(mockRemoveRole);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Open role management dialog
    const manageRolesButtons = screen.getAllByLabelText('Manage roles');
    fireEvent.click(manageRolesButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Manage User Roles')).toBeInTheDocument();
    });

    // Remove editor role
    const removeRoleButtons = screen.getAllByLabelText('Remove role');
    fireEvent.click(removeRoleButtons[0]);

    await waitFor(() => {
      expect(mockRemoveRole).toHaveBeenCalledWith('1', 'editor');
    });
  });

  it('handles search functionality', async () => {
    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
      expect(screen.getByText('jane.smith')).toBeInTheDocument();
      expect(screen.getByText('bob.wilson')).toBeInTheDocument();
    });

    // Find search input
    const searchInput = screen.getByPlaceholderText('Search users...');
    
    // Search for 'john'
    fireEvent.change(searchInput, { target: { value: 'john' } });

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
      expect(screen.queryByText('jane.smith')).not.toBeInTheDocument();
      expect(screen.queryByText('bob.wilson')).not.toBeInTheDocument();
    });
  });

  it('filters users by status', async () => {
    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
      expect(screen.getByText('bob.wilson')).toBeInTheDocument();
    });

    // Find status filter
    const statusFilter = screen.getByLabelText('Filter by Status');
    fireEvent.mouseDown(statusFilter);

    await waitFor(() => {
      expect(screen.getByText('Inactive')).toBeInTheDocument();
    });

    // Select inactive filter
    fireEvent.click(screen.getByText('Inactive'));

    await waitFor(() => {
      // Only inactive user should be visible
      expect(screen.getByText('bob.wilson')).toBeInTheDocument();
      expect(screen.queryByText('john.doe')).not.toBeInTheDocument();
      expect(screen.queryByText('jane.smith')).not.toBeInTheDocument();
    });
  });

  it('filters users by role', async () => {
    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
      expect(screen.getByText('jane.smith')).toBeInTheDocument();
    });

    // Find role filter
    const roleFilter = screen.getByLabelText('Filter by Role');
    fireEvent.mouseDown(roleFilter);

    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument();
    });

    // Select admin filter
    fireEvent.click(screen.getByText('admin'));

    await waitFor(() => {
      // Only admin user should be visible
      expect(screen.getByText('jane.smith')).toBeInTheDocument();
      expect(screen.queryByText('john.doe')).not.toBeInTheDocument();
    });
  });

  it('handles pagination correctly', async () => {
    const manyUsers = Array.from({ length: 25 }, (_, i) => ({
      id: `user-${i}`,
      username: `user${i}`,
      email: `user${i}@example.com`,
      full_name: `User ${i}`,
      is_active: true,
      is_superuser: false,
      roles: ['viewer'],
      groups: [],
      last_login: '2024-01-01T10:00:00Z',
      created_at: '2023-12-01T00:00:00Z'
    }));

    (UserService.getUsers as Mock).mockResolvedValue(manyUsers);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      // Should show pagination controls for more than 20 items
      expect(screen.getByLabelText('Go to next page')).toBeInTheDocument();
    });

    // Test page navigation
    const nextPageButton = screen.getByLabelText('Go to next page');
    fireEvent.click(nextPageButton);

    await waitFor(() => {
      // Should show page 2 items
      expect(screen.getByText('user20')).toBeInTheDocument();
    });
  });

  it('handles bulk operations correctly', async () => {
    const mockBulkDeactivateUsers = vi.fn().mockResolvedValue(true);
    (UserService.bulkDeactivateUsers as Mock).mockImplementation(mockBulkDeactivateUsers);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Select multiple users
    const checkboxes = screen.getAllByRole('checkbox');
    fireEvent.click(checkboxes[1]); // First user
    fireEvent.click(checkboxes[2]); // Second user

    // Should show bulk actions
    await waitFor(() => {
      expect(screen.getByText('2 selected')).toBeInTheDocument();
      expect(screen.getByText('Deactivate Selected')).toBeInTheDocument();
    });

    // Perform bulk deactivation
    window.confirm = vi.fn().mockReturnValue(true);
    fireEvent.click(screen.getByText('Deactivate Selected'));

    await waitFor(() => {
      expect(mockBulkDeactivateUsers).toHaveBeenCalledWith(['1', '2']);
    });
  });

  it('resets user password', async () => {
    const mockResetPassword = vi.fn().mockResolvedValue({ temporary_password: 'TempPass123!' });
    (UserService.resetUserPassword as Mock).mockImplementation(mockResetPassword);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Find and click reset password button
    const resetPasswordButtons = screen.getAllByLabelText('Reset password');
    fireEvent.click(resetPasswordButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Reset Password')).toBeInTheDocument();
    });

    // Confirm password reset
    const confirmButton = screen.getByText('Reset Password');
    fireEvent.click(confirmButton);

    await waitFor(() => {
      expect(mockResetPassword).toHaveBeenCalledWith('1');
      expect(screen.getByText('Password reset successfully')).toBeInTheDocument();
    });
  });

  it('validates form inputs correctly', async () => {
    renderWithProviders(<UserManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create User'));

    await waitFor(() => {
      expect(screen.getByText('Create New User')).toBeInTheDocument();
    });

    // Try to submit empty form
    const submitButton = screen.getByText('Create');
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(screen.getByText('Username is required')).toBeInTheDocument();
      expect(screen.getByText('Email is required')).toBeInTheDocument();
      expect(screen.getByText('Password is required')).toBeInTheDocument();
    });

    // Enter invalid email
    const emailInput = screen.getByLabelText('Email');
    fireEvent.change(emailInput, { target: { value: 'invalid-email' } });
    fireEvent.blur(emailInput);

    await waitFor(() => {
      expect(screen.getByText('Invalid email format')).toBeInTheDocument();
    });
  });

  it('handles loading state correctly', async () => {
    // Delay the API response to test loading state
    (UserService.getUsers as Mock).mockImplementation(
      () => new Promise(resolve => setTimeout(() => resolve(mockUsers), 1000))
    );

    renderWithProviders(<UserManagement />);

    // Should show loading indicator
    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    }, { timeout: 2000 });
  });

  it('handles error state correctly', async () => {
    (UserService.getUsers as Mock).mockRejectedValue(new Error('Failed to load users'));

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('Error loading users')).toBeInTheDocument();
    });
  });

  it('shows empty state when no users exist', async () => {
    (UserService.getUsers as Mock).mockResolvedValue([]);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('No users found')).toBeInTheDocument();
      expect(screen.getByText('Create your first user to get started')).toBeInTheDocument();
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

    renderWithProviders(<UserManagement />, nonAdminState);

    await waitFor(() => {
      // Should not show create button for non-admin users
      expect(screen.queryByText('Create User')).not.toBeInTheDocument();
      
      // Should not show edit/delete buttons
      expect(screen.queryByLabelText('Edit user')).not.toBeInTheDocument();
      expect(screen.queryByLabelText('Deactivate user')).not.toBeInTheDocument();
    });
  });

  it('exports user list', async () => {
    const mockExportUsers = vi.fn().mockResolvedValue('users.csv');
    (UserService.exportUsers as Mock).mockImplementation(mockExportUsers);

    renderWithProviders(<UserManagement />);

    await waitFor(() => {
      expect(screen.getByText('Export')).toBeInTheDocument();
    });

    // Click export button
    const exportButton = screen.getByText('Export');
    fireEvent.click(exportButton);

    await waitFor(() => {
      expect(mockExportUsers).toHaveBeenCalled();
    });
  });
});