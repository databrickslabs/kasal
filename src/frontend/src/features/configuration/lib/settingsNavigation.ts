import { useGroupStore } from '../../../store/groups';
import { usePermissionStore } from '../../../store/permissions';
import { useRunStatusStore } from '../../../store/runStatus';
import { isOwnPersonalWorkspace } from '../../../store/user';
import { settingsSections, type SettingsSectionId } from '../components/settingsSections';
import { getTabGroupId } from '../../../shared/api/tabGroup';

const KEY = 'kasal-settings-teamspace-navigation';
interface SettingsNavigation { groupId: string; section: SettingsSectionId }

/** Reopen the same settings section after the normal teamspace context reload. */
export function readSettingsNavigation(): SettingsNavigation | null {
  try {
    const value = JSON.parse(sessionStorage.getItem(KEY) || 'null');
    return value && value.groupId === getTabGroupId() && settingsSections.some(section => section.id === value.section) ? value : null;
  } catch { return null; }
}

export function clearSettingsNavigation() {
  sessionStorage.removeItem(KEY);
}

export function switchSettingsTeamspace(groupId: string, section: SettingsSectionId, reload = () => window.location.reload()) {
  const group = useGroupStore.getState().groups.find(candidate => candidate.id === groupId);
  const access = usePermissionStore.getState();
  const canAdminister = group && group.status === 'active' && (
    access.isSystemAdmin || group.user_role === 'ADMIN' || (isOwnPersonalWorkspace(group.id) && access.isPersonalWorkspaceManager)
  );
  if (!canAdminister) throw new Error('This teamspace is not available for administration.');
  // Use the same context change as the profile switch: clear old runs, notify
  // workspace subscribers, then reload caches and permissions for the new space.
  sessionStorage.setItem(KEY, JSON.stringify({ groupId, section }));
  useRunStatusStore.getState().clearRunHistory();
  useGroupStore.getState().setCurrentGroup(groupId);
  reload();
}
