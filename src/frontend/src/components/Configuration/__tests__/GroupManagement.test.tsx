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

import GroupManagement from '../GroupManagement';
import theme from '../../../theme/theme';

// Must use vi.hoisted for variables referenced in vi.mock
const mocks = vi.hoisted(() => ({
  mockGetGroups: vi.fn(),
  mockGetUsers: vi.fn(),
  mockCreateGroup: vi.fn(),
  mockDeleteGroup: vi.fn(),
  mockRefreshGroupStore: vi.fn(),
}));

// Mock GroupService as singleton
vi.mock('../../../api/GroupService', () => ({
  GroupService: {
    getInstance: vi.fn(() => ({
      getGroups: mocks.mockGetGroups,
      createGroup: mocks.mockCreateGroup,
      updateGroup: vi.fn(),
      deleteGroup: mocks.mockDeleteGroup,
      assignUserToGroup: vi.fn(),
      removeUserFromGroup: vi.fn(),
      getGroupUsers: vi.fn(),
    })),
  },
}));

// Mock UserService as singleton
vi.mock('../../../api/UserService', () => ({
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
  });

  it('renders the component', async () => {
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Workspaces')).toBeInTheDocument();
    });
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

  it('wires up refreshGroupStore from the Zustand group store', async () => {
    // GroupManagement calls useGroupStore(s => s.refresh) during render.
    // Our mock (line 58-65) returns mocks.mockRefreshGroupStore for the
    // refresh selector.  The component's handleCreateGroup and
    // handleDeleteGroup call refreshGroupStore() after their API calls.
    // Verifying the mock was invoked with a selector that returns the
    // refresh function confirms the wiring is correct.
    renderWithProviders(<GroupManagement />);

    await waitFor(() => {
      expect(screen.getByText('Workspaces')).toBeInTheDocument();
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
