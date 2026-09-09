/**
 * Unit tests for GroupManagement component.
 *
 * Tests the functionality of the group management interface including
 * group CRUD operations, member management, and user interactions.
 */
import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { BrowserRouter } from 'react-router-dom';
import { vi, describe, it, expect, beforeEach } from 'vitest';

import GroupManagement from './GroupManagement';
import theme from '../../../theme/theme';

// Must use vi.hoisted for variables referenced in vi.mock
const mocks = vi.hoisted(() => ({
  mockGetGroups: vi.fn(),
  mockGetUsers: vi.fn(),
  mockGetGroupUsers: vi.fn(),
  mockAssignUser: vi.fn(),
  mockCreateGroup: vi.fn(),
  mockDuplicateGroup: vi.fn(),
  mockDeleteGroup: vi.fn(),
  mockRefreshGroupStore: vi.fn(),
}));

// Mock GroupService as singleton
vi.mock('../../../api/groups/GroupService', () => ({
  GroupService: {
    getInstance: vi.fn(() => ({
      getGroups: mocks.mockGetGroups,
      createGroup: mocks.mockCreateGroup,
      duplicateGroup: mocks.mockDuplicateGroup,
      updateGroup: vi.fn(),
      deleteGroup: mocks.mockDeleteGroup,
      assignUserToGroup: mocks.mockAssignUser,
      removeUserFromGroup: vi.fn(),
      getGroupUsers: mocks.mockGetGroupUsers,
    })),
  },
}));

// Mock UserService as singleton
vi.mock('../../../api/groups/UserService', () => ({
  UserService: {
    getInstance: vi.fn(() => ({
      getUsers: mocks.mockGetUsers,
    })),
  },
}));

// Mock permission store with admin access
vi.mock('../../../store/permissions', () => ({
  usePermissionStore: vi.fn((selector) => {
    const state = {
      userRole: 'admin',
      isSystemAdmin: true,
      isLoading: false,
    };
    return selector ? selector(state) : state;
  }),
}));

// Mock group store — use the hoisted mock so tests can assert on it
vi.mock('../../../store/groups', () => ({
  useGroupStore: vi.fn((selector) => {
    const state = {
      refresh: mocks.mockRefreshGroupStore,
    };
    return selector ? selector(state) : state;
  }),
}));

const renderWithProviders = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>
      <ThemeProvider theme={theme}>
        {component}
      </ThemeProvider>
    </BrowserRouter>
  );
};

const mockGroups = [
  {
    id: '1',
    name: 'Administrators',
    description: 'System administrators group',
    is_active: true,
    user_count: 5,
    created_at: '2024-01-01T00:00:00Z'
  },
  {
    id: '2',
    name: 'Developers',
    description: 'Development team group',
    is_active: true,
    user_count: 10,
    created_at: '2024-01-02T00:00:00Z'
  }
];

const mockUsers = [
  { id: '1', username: 'admin', email: 'admin@example.com' },
  { id: '2', username: 'developer', email: 'dev@example.com' },
];

describe('GroupManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.mockGetGroups.mockResolvedValue(mockGroups);
    mocks.mockGetUsers.mockResolvedValue(mockUsers);
    mocks.mockDuplicateGroup.mockResolvedValue({ ...mockGroups[0], id: 'copy', name: 'New team' });
    mocks.mockGetGroupUsers.mockResolvedValue([]);
    mocks.mockAssignUser.mockResolvedValue({});
  });

  it('renders the component', async () => {
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Teamspaces')).toBeInTheDocument();
    });
  });

  it('duplicates from a teamspace action with the existing description and optional members', async () => {
    renderWithProviders(<GroupManagement />);
    fireEvent.click(await screen.findByLabelText('Actions for Developers'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Duplicate Teamspace' }));
    expect(screen.getByLabelText('Teamspace Name', { exact: false })).toHaveValue('Developers copy');
    expect(screen.getByLabelText('Description (Optional)')).toHaveValue('Development team group');
    fireEvent.change(screen.getByLabelText('Teamspace Name', { exact: false }), { target: { value: 'New team' } });
    fireEvent.click(screen.getByLabelText('Copy members and their roles and permissions'));
    fireEvent.click(screen.getByRole('button', { name: 'Duplicate Teamspace' }));
    await waitFor(() => expect(mocks.mockDuplicateGroup).toHaveBeenCalledWith('2', {
      name: 'New team', description: 'Development team group', include_members: false,
    }));
    expect(mocks.mockCreateGroup).not.toHaveBeenCalled();
    await waitFor(() => expect(mocks.mockRefreshGroupStore).toHaveBeenCalled());
  });

  it('offers an existing teamspace as the starting point when creating', async () => {
    renderWithProviders(<GroupManagement />);
    await screen.findByText('Developers');
    fireEvent.click(screen.getByLabelText('Create teamspace'));
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Start from' }));
    fireEvent.click(screen.getByRole('option', { name: 'Duplicate Developers' }));
    expect(screen.getByLabelText('Copy members and their roles and permissions')).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: 'Duplicate Teamspace' }));
    await waitFor(() => expect(mocks.mockDuplicateGroup).toHaveBeenCalledWith('2', {
      name: 'Developers copy', description: 'Development team group', include_members: true,
    }));
  });

  it('keeps the duplication form available on failure', async () => {
    mocks.mockDuplicateGroup.mockRejectedValueOnce(new Error('Failed copy'));
    renderWithProviders(<GroupManagement />);
    fireEvent.click(await screen.findByLabelText('Actions for Developers'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Duplicate Teamspace' }));
    fireEvent.click(screen.getByRole('button', { name: 'Duplicate Teamspace' }));
    expect(await screen.findByText('Failed to duplicate teamspace. Please try again.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Duplicate Teamspace' })).toBeEnabled();
    expect(screen.getByLabelText('Teamspace Name', { exact: false })).toHaveValue('Developers copy');
  });

  it('starts empty after cancelling a duplicate', async () => {
    renderWithProviders(<GroupManagement />);
    fireEvent.click(await screen.findByLabelText('Actions for Developers'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Duplicate Teamspace' }));
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    fireEvent.click(screen.getByLabelText('Create teamspace'));
    expect(screen.getByLabelText('Teamspace Name', { exact: false })).toHaveValue('');
    fireEvent.change(screen.getByLabelText('Teamspace Name', { exact: false }), { target: { value: 'Empty' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create Teamspace', exact: true }));
    await waitFor(() => expect(mocks.mockCreateGroup).toHaveBeenCalledWith({ name: 'Empty', description: '' }));
    expect(mocks.mockDuplicateGroup).not.toHaveBeenCalled();
  });

  it('displays loading state initially', () => {
    renderWithProviders(<GroupManagement />);

    // The component should fetch groups
    expect(mocks.mockGetGroups).toHaveBeenCalled();
  });

  it('displays groups after loading', async () => {
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Administrators')).toBeInTheDocument();
      expect(screen.getByText('Developers')).toBeInTheDocument();
    });
  });

  it('handles error when loading groups fails', async () => {
    mocks.mockGetGroups.mockRejectedValue(new Error('Failed to load'));

    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      // Component should handle error gracefully
      expect(mocks.mockGetGroups).toHaveBeenCalled();
    });
  });

  it('adds a member by email before their first login', async () => {
    mocks.mockGetUsers.mockResolvedValue([]);
    renderWithProviders(<GroupManagement />);
    fireEvent.click(await screen.findByText('Developers'));
    fireEvent.click(await screen.findByRole('button', { name: 'Add User' }));
    fireEvent.change(screen.getByRole('combobox', { name: 'Email addresses' }), {
      target: { value: 'new.person@example.com' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Add 1 Member' }));
    await waitFor(() => expect(mocks.mockAssignUser).toHaveBeenCalledWith('2', {
      user_email: 'new.person@example.com', role: 'operator',
    }));
  });

  it('deduplicates an entered address before assigning membership', async () => {
    renderWithProviders(<GroupManagement />);
    fireEvent.click(await screen.findByText('Developers'));
    fireEvent.click(await screen.findByRole('button', { name: 'Add User' }));
    const input = screen.getByRole('combobox', { name: 'Email addresses' });
    fireEvent.change(input, { target: { value: 'new.person@example.com' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    fireEvent.change(input, { target: { value: 'new.person@example.com' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add 1 Member' }));
    await waitFor(() => expect(mocks.mockAssignUser).toHaveBeenCalledTimes(1));
  });

  it('does not submit an incomplete email address', async () => {
    renderWithProviders(<GroupManagement />);
    fireEvent.click(await screen.findByText('Developers'));
    fireEvent.click(await screen.findByRole('button', { name: 'Add User' }));
    fireEvent.change(screen.getByRole('combobox', { name: 'Email addresses' }), {
      target: { value: 'new.person@' },
    });
    expect(screen.getByRole('button', { name: 'Add 1 Member' })).toBeDisabled();
    expect(mocks.mockAssignUser).not.toHaveBeenCalled();
  });

  it('wires up refreshGroupStore from the Zustand group store', async () => {
    // GroupManagement calls useGroupStore(s => s.refresh) during render.
    // Our mock (line 58-65) returns mocks.mockRefreshGroupStore for the
    // refresh selector.  The component's handleCreateGroup and
    // handleDeleteGroup call refreshGroupStore() after their API calls.
    // Verifying the mock was invoked with a selector that returns the
    // refresh function confirms the wiring is correct.
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Teamspaces')).toBeInTheDocument();
    });

    // The mock useGroupStore was called with selector functions during render.
    // Verify one of those selectors correctly extracts 'refresh'.
    const { useGroupStore } = await import('../../../store/groups');
    const mockedStore = vi.mocked(useGroupStore);
    const refreshSelector = mockedStore.mock.calls.find(
      (call) => {
        if (typeof call[0] === 'function') {
          return call[0]({ refresh: mocks.mockRefreshGroupStore } as any) === mocks.mockRefreshGroupStore;
        }
        return false;
      }
    );
    expect(refreshSelector).toBeDefined();
  });
});
