/**
 * Unit tests for GroupManagement component.
 * 
 * Tests the functionality of the group management interface including
 * group CRUD operations, member management, and user interactions.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { ThemeProvider } from '@mui/material/styles';
import { BrowserRouter } from 'react-router-dom';
import configureStore from 'redux-mock-store';
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';

import GroupManagement from '../GroupManagement';
import * as GroupService from '../../../api/GroupService';
import theme from '../../../theme';

// Mock the GroupService
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

const mockGroups = [
  {
    id: '1',
    name: 'Administrators',
    description: 'System administrators group',
    is_active: true,
    member_count: 5,
    created_at: '2024-01-01T00:00:00Z'
  },
  {
    id: '2', 
    name: 'Developers',
    description: 'Development team group',
    is_active: true,
    member_count: 12,
    created_at: '2024-01-02T00:00:00Z'
  }
];

const mockUsers = [
  {
    id: 'user1',
    username: 'john.doe',
    email: 'john@example.com',
    full_name: 'John Doe',
    is_active: true
  },
  {
    id: 'user2',
    username: 'jane.smith', 
    email: 'jane@example.com',
    full_name: 'Jane Smith',
    is_active: true
  }
];

describe('GroupManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (GroupService.getGroups as Mock).mockResolvedValue(mockGroups);
    (GroupService.getUsers as Mock).mockResolvedValue(mockUsers);
  });

  it('renders group management interface correctly', async () => {
    renderWithProviders(<GroupManagement />);

    expect(screen.getByText('Group Management')).toBeInTheDocument();
    expect(screen.getByText('Create Group')).toBeInTheDocument();
    
    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
      expect(screen.getByText('Developers')).toBeInTheDocument();
    });
  });

  it('displays groups in table format', async () => {
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Group Name')).toBeInTheDocument();
      expect(screen.getByText('Description')).toBeInTheDocument();
      expect(screen.getByText('Members')).toBeInTheDocument();
      expect(screen.getByText('Status')).toBeInTheDocument();
      expect(screen.getByText('Actions')).toBeInTheDocument();
    });
  });

  it('opens create group dialog when create button is clicked', async () => {
    renderWithProviders(<GroupManagement />);

    const createButton = screen.getByText('Create Group');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('Create New Group')).toBeInTheDocument();
      expect(screen.getByLabelText('Group Name')).toBeInTheDocument();
      expect(screen.getByLabelText('Description')).toBeInTheDocument();
    });
  });

  it('creates a new group successfully', async () => {
    const mockCreateGroup = vi.fn().mockResolvedValue({
      id: '3',
      name: 'New Group',
      description: 'A new test group',
      is_active: true
    });
    (GroupService.createGroup as Mock).mockImplementation(mockCreateGroup);

    renderWithProviders(<GroupManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create Group'));

    await waitFor(() => {
      expect(screen.getByText('Create New Group')).toBeInTheDocument();
    });

    // Fill form
    const nameInput = screen.getByLabelText('Group Name');
    const descriptionInput = screen.getByLabelText('Description');
    
    fireEvent.change(nameInput, { target: { value: 'New Group' } });
    fireEvent.change(descriptionInput, { target: { value: 'A new test group' } });

    // Submit form
    const submitButton = screen.getByText('Create');
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(mockCreateGroup).toHaveBeenCalledWith({
        name: 'New Group',
        description: 'A new test group',
        is_active: true
      });
    });
  });

  it('opens edit group dialog when edit button is clicked', async () => {
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    });

    // Find and click edit button for first group
    const editButtons = screen.getAllByLabelText('Edit group');
    fireEvent.click(editButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Edit Group')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Administrators')).toBeInTheDocument();
    });
  });

  it('updates group successfully', async () => {
    const mockUpdateGroup = vi.fn().mockResolvedValue({
      ...mockGroups[0],
      description: 'Updated description'
    });
    (GroupService.updateGroup as Mock).mockImplementation(mockUpdateGroup);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    });

    // Open edit dialog
    const editButtons = screen.getAllByLabelText('Edit group');
    fireEvent.click(editButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Edit Group')).toBeInTheDocument();
    });

    // Update description
    const descriptionInput = screen.getByLabelText('Description');
    fireEvent.change(descriptionInput, { target: { value: 'Updated description' } });

    // Submit form
    const saveButton = screen.getByText('Save');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockUpdateGroup).toHaveBeenCalledWith('1', {
        description: 'Updated description'
      });
    });
  });

  it('deletes group with confirmation', async () => {
    const mockDeleteGroup = vi.fn().mockResolvedValue(true);
    (GroupService.deleteGroup as Mock).mockImplementation(mockDeleteGroup);

    // Mock window.confirm
    window.confirm = vi.fn().mockReturnValue(true);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    });

    // Find and click delete button
    const deleteButtons = screen.getAllByLabelText('Delete group');
    fireEvent.click(deleteButtons[0]);

    await waitFor(() => {
      expect(window.confirm).toHaveBeenCalledWith('Are you sure you want to delete this group?');
      expect(mockDeleteGroup).toHaveBeenCalledWith('1');
    });
  });

  it('opens member management dialog when manage members is clicked', async () => {
    const mockGetGroupMembers = vi.fn().mockResolvedValue(mockUsers);
    (GroupService.getGroupMembers as Mock).mockImplementation(mockGetGroupMembers);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    });

    // Find and click manage members button
    const manageMembersButtons = screen.getAllByLabelText('Manage members');
    fireEvent.click(manageMembersButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Manage Group Members')).toBeInTheDocument();
      expect(mockGetGroupMembers).toHaveBeenCalledWith('1');
    });
  });

  it('adds member to group successfully', async () => {
    const mockGetGroupMembers = vi.fn().mockResolvedValue([]);
    const mockAddMemberToGroup = vi.fn().mockResolvedValue(true);
    (GroupService.getGroupMembers as Mock).mockImplementation(mockGetGroupMembers);
    (GroupService.addMemberToGroup as Mock).mockImplementation(mockAddMemberToGroup);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    });

    // Open member management dialog
    const manageMembersButtons = screen.getAllByLabelText('Manage members');
    fireEvent.click(manageMembersButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Manage Group Members')).toBeInTheDocument();
    });

    // Find and click add member button
    const addMemberButton = screen.getByText('Add Member');
    fireEvent.click(addMemberButton);

    await waitFor(() => {
      expect(screen.getByText('Add Member to Group')).toBeInTheDocument();
    });

    // Select user and add
    const userSelect = screen.getByLabelText('Select User');
    fireEvent.mouseDown(userSelect);

    await waitFor(() => {
      expect(screen.getByText('John Doe')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('John Doe'));

    const addButton = screen.getByText('Add');
    fireEvent.click(addButton);

    await waitFor(() => {
      expect(mockAddMemberToGroup).toHaveBeenCalledWith('1', 'user1');
    });
  });

  it('removes member from group successfully', async () => {
    const mockGetGroupMembers = vi.fn().mockResolvedValue(mockUsers);
    const mockRemoveMemberFromGroup = vi.fn().mockResolvedValue(true);
    (GroupService.getGroupMembers as Mock).mockImplementation(mockGetGroupMembers);
    (GroupService.removeMemberFromGroup as Mock).mockImplementation(mockRemoveMemberFromGroup);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    });

    // Open member management dialog
    const manageMembersButtons = screen.getAllByLabelText('Manage members');
    fireEvent.click(manageMembersButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Manage Group Members')).toBeInTheDocument();
      expect(screen.getByText('John Doe')).toBeInTheDocument();
    });

    // Find and click remove member button
    const removeMemberButtons = screen.getAllByLabelText('Remove member');
    fireEvent.click(removeMemberButtons[0]);

    await waitFor(() => {
      expect(mockRemoveMemberFromGroup).toHaveBeenCalledWith('1', 'user1');
    });
  });

  it('handles search functionality', async () => {
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
      expect(screen.getByText('Developers')).toBeInTheDocument();
    });

    // Find search input
    const searchInput = screen.getByPlaceholderText('Search groups...');
    
    // Search for 'Admin'
    fireEvent.change(searchInput, { target: { value: 'Admin' } });

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
      expect(screen.queryByText('Developers')).not.toBeInTheDocument();
    });
  });

  it('handles pagination correctly', async () => {
    const manyGroups = Array.from({ length: 25 }, (_, i) => ({
      id: `group-${i}`,
      name: `Group ${i}`,
      description: `Description ${i}`,
      is_active: true,
      member_count: i,
      created_at: '2024-01-01T00:00:00Z'
    }));

    (GroupService.getGroups as Mock).mockResolvedValue(manyGroups);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      // Should show pagination controls for more than 20 items
      expect(screen.getByLabelText('Go to next page')).toBeInTheDocument();
    });

    // Test page navigation
    const nextPageButton = screen.getByLabelText('Go to next page');
    fireEvent.click(nextPageButton);

    await waitFor(() => {
      // Should show page 2 items
      expect(screen.getByText('Group 20')).toBeInTheDocument();
    });
  });

  it('handles loading state correctly', async () => {
    // Delay the API response to test loading state
    (GroupService.getGroups as Mock).mockImplementation(
      () => new Promise(resolve => setTimeout(() => resolve(mockGroups), 1000))
    );

    renderWithProviders(<GroupManagement />);

    // Should show loading indicator
    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    }, { timeout: 2000 });
  });

  it('handles error state correctly', async () => {
    (GroupService.getGroups as Mock).mockRejectedValue(new Error('Failed to load groups'));

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Error loading groups')).toBeInTheDocument();
    });
  });

  it('shows empty state when no groups exist', async () => {
    (GroupService.getGroups as Mock).mockResolvedValue([]);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('No groups found')).toBeInTheDocument();
      expect(screen.getByText('Create your first group to get started')).toBeInTheDocument();
    });
  });

  it('validates form inputs correctly', async () => {
    renderWithProviders(<GroupManagement />);

    // Open create dialog
    fireEvent.click(screen.getByText('Create Group'));

    await waitFor(() => {
      expect(screen.getByText('Create New Group')).toBeInTheDocument();
    });

    // Try to submit empty form
    const submitButton = screen.getByText('Create');
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(screen.getByText('Group name is required')).toBeInTheDocument();
    });

    // Enter name that's too short
    const nameInput = screen.getByLabelText('Group Name');
    fireEvent.change(nameInput, { target: { value: 'AB' } });
    fireEvent.blur(nameInput);

    await waitFor(() => {
      expect(screen.getByText('Group name must be at least 3 characters')).toBeInTheDocument();
    });
  });

  it('handles permissions correctly for non-admin users', async () => {
    const nonAdminState = {
      auth: {
        user: {
          id: '123',
          username: 'testuser',
          roles: ['user'],
          is_superuser: false
        }
      }
    };

    renderWithProviders(<GroupManagement />, nonAdminState);

    await waitFor(() => {
      // Should not show create button for non-admin users
      expect(screen.queryByText('Create Group')).not.toBeInTheDocument();
      
      // Should not show edit/delete buttons
      expect(screen.queryByLabelText('Edit group')).not.toBeInTheDocument();
      expect(screen.queryByLabelText('Delete group')).not.toBeInTheDocument();
    });
  });

  it('handles bulk operations correctly', async () => {
    const mockBulkDeleteGroups = vi.fn().mockResolvedValue(true);
    (GroupService.bulkDeleteGroups as Mock).mockImplementation(mockBulkDeleteGroups);

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
    });

    // Select multiple groups
    const checkboxes = screen.getAllByRole('checkbox');
    fireEvent.click(checkboxes[1]); // First group
    fireEvent.click(checkboxes[2]); // Second group

    // Should show bulk actions
    await waitFor(() => {
      expect(screen.getByText('2 selected')).toBeInTheDocument();
      expect(screen.getByText('Delete Selected')).toBeInTheDocument();
    });

    // Perform bulk delete
    window.confirm = vi.fn().mockReturnValue(true);
    fireEvent.click(screen.getByText('Delete Selected'));

    await waitFor(() => {
      expect(mockBulkDeleteGroups).toHaveBeenCalledWith(['1', '2']);
    });
  });
});