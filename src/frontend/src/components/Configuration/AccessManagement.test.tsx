import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import AccessManagement from './AccessManagement';

const getUsers = vi.fn();
const updateUserPermissions = vi.fn();
vi.mock('../../api/groups/UserService', () => ({
  UserService: { getInstance: () => ({ getUsers, updateUserPermissions }) },
}));

const getGroups = vi.fn();
const getGroupUsers = vi.fn();
const updateGroupUser = vi.fn();
const assignUserToGroup = vi.fn();
const removeUserFromGroup = vi.fn();
vi.mock('../../api/groups/GroupService', () => ({
  GroupService: {
    getInstance: () => ({
      getGroups,
      getGroupUsers,
      updateGroupUser,
      assignUserToGroup,
      removeUserFromGroup,
    }),
  },
}));

vi.mock('./GroupManagement', () => ({
  default: () => <div data-testid="group-management" />,
}));

const USER = {
  id: 'u1',
  email: 'ada@example.com',
  role: 'user',
  status: 'active',
  is_system_admin: false,
  is_personal_workspace_manager: false,
  created_at: '',
  updated_at: '',
  last_login: null,
};
const GROUP = { id: 'g1', name: 'Research', status: 'active', auto_created: false, created_at: '', updated_at: '' };
const GROUP2 = { id: 'g2', name: 'Ops', status: 'active', auto_created: false, created_at: '', updated_at: '' };
const MEMBER = {
  id: 'm1', group_id: 'g1', user_id: 'uid-1', email: 'ada@example.com',
  role: 'operator', status: 'active', joined_at: '', auto_created: false, created_at: '', updated_at: '',
};

describe('AccessManagement (combined Access screen)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getUsers.mockResolvedValue([USER]);
    getGroups.mockResolvedValue([GROUP, GROUP2]);
    getGroupUsers.mockImplementation(async (id: string) => (id === 'g1' ? [MEMBER] : []));
    updateUserPermissions.mockResolvedValue({ ...USER, is_system_admin: true });
    updateGroupUser.mockResolvedValue({});
    assignUserToGroup.mockResolvedValue({});
    removeUserFromGroup.mockResolvedValue(undefined);
  });

  it('People lens shows the user with global toggles and membership role chips', async () => {
    render(<AccessManagement />);
    expect(await screen.findByText('ada@example.com')).toBeInTheDocument();
    expect(screen.getByText('Research: operator · chat only')).toBeInTheDocument();
    expect(screen.getByLabelText('System Admin for ada@example.com')).toBeInTheDocument();
  });

  it('toggling System Admin calls the user service', async () => {
    render(<AccessManagement />);
    await screen.findByText('ada@example.com');
    fireEvent.click(screen.getByLabelText('System Admin for ada@example.com'));
    await waitFor(() =>
      expect(updateUserPermissions).toHaveBeenCalledWith('u1', { is_system_admin: true }),
    );
  });

  it('a role chip opens the role menu; picking a role updates the membership', async () => {
    render(<AccessManagement />);
    fireEvent.click(await screen.findByText('Research: operator · chat only'));
    // Operator is annotated with what it now MEANS for the surfaces.
    expect(screen.getByText('Chat only — no builders')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Editor'));
    await waitFor(() =>
      expect(updateGroupUser).toHaveBeenCalledWith('g1', 'uid-1', {
        role: 'editor',
        allow_agent_builder: true,
        allow_flow_builder: true,
      }),
    );
  });

  it('the chip menu shows NAMED builder switches and toggles an override', async () => {
    render(<AccessManagement />);
    fireEvent.click(await screen.findByText('Research: operator · chat only'));
    expect(screen.getByText('Agent Builder')).toBeInTheDocument();
    expect(screen.getByText('Flow Builder')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Agent Builder'));
    await waitFor(() =>
      expect(updateGroupUser).toHaveBeenCalledWith('g1', 'uid-1', {
        allow_agent_builder: true,
      }),
    );
  });

  it('the chip menu can remove the membership', async () => {
    render(<AccessManagement />);
    fireEvent.click(await screen.findByText('Research: operator · chat only'));
    fireEvent.click(screen.getByText('Remove from teamspace'));
    await waitFor(() => expect(removeUserFromGroup).toHaveBeenCalledWith('g1', 'uid-1'));
  });

  it('the "+" adds the user to a not-yet-joined teamspace as operator', async () => {
    render(<AccessManagement />);
    await screen.findByText('ada@example.com');
    fireEvent.click(screen.getByLabelText('Add ada@example.com to a teamspace'));
    fireEvent.click(screen.getByText('Ops'));
    await waitFor(() =>
      expect(assignUserToGroup).toHaveBeenCalledWith('g2', {
        user_email: 'ada@example.com',
        role: 'operator',
      }),
    );
  });

  it('the Teamspaces lens hosts the existing GroupManagement', async () => {
    render(<AccessManagement />);
    await screen.findByText('ada@example.com');
    fireEvent.click(screen.getByRole('tab', { name: /Teamspaces/ }));
    expect(screen.getByTestId('group-management')).toBeInTheDocument();
  });
});
