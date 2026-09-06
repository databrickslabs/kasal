import { beforeEach, describe, expect, it, vi } from 'vitest';
import { clearSettingsNavigation, readSettingsNavigation, switchSettingsTeamspace } from './settingsNavigation';
import { useGroupStore } from '../../../store/groups';
import { usePermissionStore } from '../../../store/permissions';
import { useRunStatusStore } from '../../../store/runStatus';

beforeEach(() => {
  localStorage.clear(); sessionStorage.clear();
  usePermissionStore.setState({ isSystemAdmin: false, isPersonalWorkspaceManager: false });
  useRunStatusStore.setState({ clearRunHistory: vi.fn() });
  useGroupStore.setState({ currentGroupId: 'old-team', groups: [
    { id: 'admin-team', name: 'Admin team', user_role: 'ADMIN', status: 'active', auto_created: false, user_count: 1, created_at: '', updated_at: '' },
    { id: 'editor-team', name: 'Editor team', user_role: 'EDITOR', status: 'active', auto_created: false, user_count: 1, created_at: '', updated_at: '' },
  ] });
});
describe('teamspace settings navigation', () => {
  it('switches actual context, clears old runs, and remembers the section across reload', () => {
    const reload = vi.fn(); const changed = vi.fn();
    window.addEventListener('group-changed', changed);
    try {
      switchSettingsTeamspace('admin-team', 'models', reload);
      expect(localStorage.getItem('selectedGroupId')).toBe('admin-team');
      expect(useGroupStore.getState().currentGroupId).toBe('admin-team');
      expect(useRunStatusStore.getState().clearRunHistory).toHaveBeenCalledOnce();
      expect(changed).toHaveBeenCalledOnce();
      expect(reload).toHaveBeenCalledOnce();
      expect(readSettingsNavigation()).toEqual({ groupId: 'admin-team', section: 'models' });
      clearSettingsNavigation();
      expect(readSettingsNavigation()).toBeNull();
    } finally { window.removeEventListener('group-changed', changed); }
  });
  it('rejects a teamspace the user does not administer', () => {
    const reload = vi.fn();
    expect(() => switchSettingsTeamspace('editor-team', 'models', reload)).toThrow('not available');
    expect(reload).not.toHaveBeenCalled();
    expect(useGroupStore.getState().currentGroupId).toBe('old-team');
  });
  it('does not reopen another teamspace’s settings if context has changed', () => {
    switchSettingsTeamspace('admin-team', 'models', vi.fn());
    localStorage.setItem('selectedGroupId', 'old-team');
    expect(readSettingsNavigation()).toBeNull();
  });
});
