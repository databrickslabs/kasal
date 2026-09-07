import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore, type AppMode } from '../../store/uiLayout';
import { usePermissionStore } from '../../store/permissions';
import { useSessionStore, cancelSessionNavigation } from '../../features/chat/store/sessionStore';
import { useExecutionStore } from '../../features/chat/store/executionStore';
import { useSessionPreferences } from './sessionPreferences';
import type { WorkspaceSession } from './sessionIndex';

export const modeLabels: Record<AppMode, string> = { chat: 'Chat', crew: 'Agent Builder', flow: 'Flow Builder' };

let navigationVersion = 0;

function allowed(mode: AppMode) {
  const permissions = usePermissionStore.getState();
  return mode === 'chat' || (mode === 'crew' ? permissions.canUseAgentBuilder() : permissions.canUseFlowBuilder());
}

function saveChat() {
  const id = useSessionStore.getState().currentSessionId;
  if (id) useExecutionStore.getState().saveSessionState(id);
}

export function newWorkspaceSession(mode: AppMode) {
  if (!allowed(mode)) return;
  navigationVersion += 1;
  cancelSessionNavigation();
  if (useUILayoutStore.getState().appMode === 'chat') saveChat();
  if (mode === 'chat') {
    useSessionStore.getState().startNewChat();
    useExecutionStore.getState().resetForSession();
  } else {
    useTabManagerStore.getState().createTab(`New ${mode === 'crew' ? 'crew' : 'flow'}`, mode, { sessionDraft: true });
  }
  useUILayoutStore.getState().setAppMode(mode);
  if (mode === 'crew') useUILayoutStore.getState().setAssistantPanelVisible(true);
  if (mode === 'flow') useUILayoutStore.getState().setFlowPanelTab('crews');
}

export async function openWorkspaceSession(session: WorkspaceSession) {
  if (!allowed(session.mode)) return;
  const version = ++navigationVersion;
  cancelSessionNavigation();
  if (useUILayoutStore.getState().appMode === 'chat') saveChat();
  if (session.mode === 'chat') {
    // Load before changing the visible mode, so a previous chat cannot flash.
    await useSessionStore.getState().switchSession(session.id);
    if (version !== navigationVersion || useSessionStore.getState().currentSessionId !== session.id) return;
    useExecutionStore.getState().restoreSessionState(session.id);
  } else {
    const tab = useTabManagerStore.getState().getTab(session.id);
    if (!tab || tab.group_id !== (localStorage.getItem('selectedGroupId') || '')) return;
    useTabManagerStore.getState().setActiveTab(tab.id);
  }
  useUILayoutStore.getState().setAppMode(session.mode);
  if (session.mode !== 'chat') useUILayoutStore.getState().setAssistantPanelVisible(true);
}

/** Mode selection opens existing work of that kind, never relabels a populated canvas. */
export function switchWorkspaceMode(mode: AppMode) {
  if (!allowed(mode)) return;
  navigationVersion += 1;
  cancelSessionNavigation();
  if (mode === 'chat') {
    useUILayoutStore.getState().setAppMode(mode);
    return;
  }
  const store = useTabManagerStore.getState();
  const preferences = useSessionPreferences.getState().entries;
  const matching = store.getTabsForCurrentGroup()
    .filter(tab => tab.viewMode === mode && !preferences[`builder:${tab.id}`]?.archived)
    .sort((a, b) => Number(b.id === store.activeTabId) - Number(a.id === store.activeTabId)
      || new Date(b.lastModified).getTime() - new Date(a.lastModified).getTime());
  if (matching[0]) {
    store.setActiveTab(matching[0].id);
    useUILayoutStore.getState().setAppMode(mode);
  } else newWorkspaceSession(mode);
}
