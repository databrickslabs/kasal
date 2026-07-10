import React from 'react';
import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import SharedWorkspaceNotice from './SharedWorkspaceNotice';
import { useGroupStore } from '../../../../store/groups';
import type { GroupWithRole } from '../../../../api/GroupService';

const SHARED: GroupWithRole = {
  id: 'team_acme',
  name: 'Acme Team',
  status: 'active',
  user_count: 4,
} as GroupWithRole;

const PERSONAL: GroupWithRole = {
  id: 'user_alice_acme_com',
  name: 'My Workspace',
  status: 'active',
  user_count: 1,
} as GroupWithRole;

const setWorkspace = (groupId: string | null, groups: GroupWithRole[]) =>
  useGroupStore.setState({ currentGroupId: groupId, groups });

beforeEach(() => {
  localStorage.clear();
  setWorkspace(null, []);
});

describe('SharedWorkspaceNotice', () => {
  it('does not render in a personal workspace', () => {
    setWorkspace(PERSONAL.id, [PERSONAL]);
    render(<SharedWorkspaceNotice />);
    expect(screen.queryByTestId('shared-workspace-notice')).toBeNull();
  });

  it('does not render when no workspace is selected', () => {
    render(<SharedWorkspaceNotice />);
    expect(screen.queryByTestId('shared-workspace-notice')).toBeNull();
  });

  it('renders in a shared workspace and names the member count', () => {
    setWorkspace(SHARED.id, [SHARED]);
    render(<SharedWorkspaceNotice />);
    const notice = screen.getByTestId('shared-workspace-notice');
    expect(notice).toBeInTheDocument();
    expect(notice).toHaveTextContent('all 4 members');
  });

  it('falls back to "all members" when member count is unknown', () => {
    setWorkspace('team_no_count', [{ id: 'team_no_count', name: 'X', status: 'active' } as GroupWithRole]);
    render(<SharedWorkspaceNotice />);
    expect(screen.getByTestId('shared-workspace-notice')).toHaveTextContent('all members');
  });

  it('stays dismissed for that workspace after dismissal (once per workspace)', () => {
    setWorkspace(SHARED.id, [SHARED]);
    const { unmount } = render(<SharedWorkspaceNotice />);
    fireEvent.click(screen.getByLabelText('Dismiss shared teamspace notice'));
    expect(screen.queryByTestId('shared-workspace-notice')).toBeNull();
    expect(localStorage.getItem(`kasal_shared_ws_notice_dismissed:${SHARED.id}`)).toBe('1');

    // Re-mounting (e.g. reload) keeps it dismissed for the same workspace.
    unmount();
    render(<SharedWorkspaceNotice />);
    expect(screen.queryByTestId('shared-workspace-notice')).toBeNull();
  });

  it('still shows for a different shared workspace after dismissing another', () => {
    localStorage.setItem(`kasal_shared_ws_notice_dismissed:${SHARED.id}`, '1');
    const other: GroupWithRole = { id: 'team_other', name: 'Other', status: 'active', user_count: 2 } as GroupWithRole;
    setWorkspace(other.id, [SHARED, other]);
    render(<SharedWorkspaceNotice />);
    expect(screen.getByTestId('shared-workspace-notice')).toBeInTheDocument();
  });
});
