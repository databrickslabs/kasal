import { useBuilderCanvasStore } from './builderCanvasStore';
import { useUILayoutStore, type AppMode } from '../../store/uiLayout';
import { usePermissionStore } from '../../store/permissions';
import { useSessionStore, cancelSessionNavigation } from './sessionStore';
import { useExecutionStore } from '../../features/chat/store/executionStore';
import { useSessionPreferences } from './sessionPreferences';
import { builderSessionKey, type WorkspaceSession } from './sessionIndex';

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
    useBuilderCanvasStore.getState().createCanvas(`New ${mode === 'crew' ? 'crew' : 'flow'}`, mode, { sessionDraft: true });
  }
  useUILayoutStore.getState().setAppMode(mode);
  if (mode === 'crew') useUILayoutStore.getState().setAssistantPanelVisible(true);
  if (mode === 'flow') useUILayoutStore.getState().setFlowPanelTab('crews');
}

export async function openWorkspaceSession(session: WorkspaceSession) {
  if (!allowed(session.mode) || session.canvasPending) return;
  const version = ++navigationVersion;
  cancelSessionNavigation();
  if (useUILayoutStore.getState().appMode === 'chat') saveChat();
  if (session.mode === 'chat') {
    // Load before changing the visible mode, so a previous chat cannot flash.
    await useSessionStore.getState().switchSession(session.id);
    if (version !== navigationVersion || useSessionStore.getState().currentSessionId !== session.id) return;
    useExecutionStore.getState().restoreSessionState(session.id);
  } else {
    const tab = useBuilderCanvasStore.getState().getCanvas(session.id);
    if (!tab || useBuilderCanvasStore.getState().unavailableSessionIds.includes(tab.chatSessionId || tab.id) || tab.group_id !== (localStorage.getItem('selectedGroupId') || '')) return;
    useBuilderCanvasStore.getState().setActiveCanvas(tab.id);
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
  const store = useBuilderCanvasStore.getState();
  const preferences = useSessionPreferences.getState().entries;
  const matching = store.getCanvasesForCurrentGroup()
    .filter(tab => tab.viewMode === mode && !preferences[builderSessionKey(tab)]?.archived
      && !store.unavailableSessionIds.includes(tab.chatSessionId || tab.id))
    .sort((a, b) => Number(b.id === store.activeCanvasId) - Number(a.id === store.activeCanvasId)
      || new Date(b.lastModified).getTime() - new Date(a.lastModified).getTime());
  if (matching[0]) {
    store.setActiveCanvas(matching[0].id);
    useUILayoutStore.getState().setAppMode(mode);
  } else newWorkspaceSession(mode);
}
