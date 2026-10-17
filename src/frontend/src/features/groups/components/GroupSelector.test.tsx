import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import GroupSelector from './GroupSelector';
import { useUserStore } from '../../../store/user';
import { useGroupStore } from '../../../store/groups';
import {
  getTabGroupId, markTabGroupAccessLost, setTabGroupId, tabGroupAccessLost,
} from '../../../shared/api/tabGroup';

vi.mock('react-hot-toast', () => ({ default: { success: vi.fn(), error: vi.fn() } }));
vi.mock('../../../api/groups/GroupService', () => ({
  GroupService: {
    getInstance: () => ({
      getMyGroups: vi.fn().mockResolvedValue([{ id: 'team_t', name: 'Team T', status: 'active', user_role: 'EDITOR' }]),
    }),
  },
}));

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  // This tab works in Team T.
  setTabGroupId('team_t');
  useGroupStore.setState({ currentGroupId: 'team_t' });
  useUserStore.setState({
    currentUser: { email: 'person@example.com', personal_group_id: 'personal' } as never,
    isLoading: false,
  });
});

it('lets the user pick the teamspace again after this tab lost access to it', async () => {
  render(<GroupSelector />);
  const button = await screen.findByRole('button', { name: /Switch teamspace: Team T/ });
  expect(getTabGroupId()).toBe('team_t');

  // The server refused the tab's teamspace (the API client does this on a 403).
  markTabGroupAccessLost('team_t');
  expect(tabGroupAccessLost()).toBe(true);

  fireEvent.click(button);
  fireEvent.click(await screen.findByText('Team T'));

  expect(getTabGroupId()).toBe('team_t');
  expect(tabGroupAccessLost()).toBe(false);
});
