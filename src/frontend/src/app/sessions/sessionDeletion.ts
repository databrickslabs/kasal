import { isAxiosError } from 'axios';
import { deleteSession } from '../../features/chat/persistence/sessionApi';
import { useSessionStore } from './sessionStore';
import { useExecutionStore } from '../../features/chat/store/executionStore';
import { useChatMessagesStore } from '../../features/workflow/assistant/store/chatMessagesStore';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useUILayoutStore } from '../../store/uiLayout';
import { switchWorkspaceMode } from './sessionNavigation';
import { useSessionPreferences } from './sessionPreferences';
import type { WorkspaceSession } from './sessionIndex';
import { pauseBuilderSessionWrites } from './builderSessionLifecycle';

/** Delete a session, not its saved catalog configuration or execution records. */
export async function deleteArchivedSession(session: WorkspaceSession, groupId: string) {
  if (groupId !== (localStorage.getItem('selectedGroupId') || '')) {
    throw new Error('The teamspace changed. Reopen the archive and try again.');
  }
  const tab = session.mode === 'chat' ? undefined : useBuilderCanvasStore.getState().getCanvas(session.id);
  const chatId = session.mode === 'chat' ? session.id : tab?.chatSessionId;
  const chat = useSessionStore.getState().sessions.find(item => item.id === chatId);
  if (!useSessionPreferences.getState().entries[session.key]?.archived
    || (session.mode === 'chat' ? !chat || (chat.groupId && chat.groupId !== groupId) : !tab || tab.group_id !== groupId)) {
    throw new Error('This session is no longer in this archive.');
  }
  if (tab?.executionStatus === 'running' || chat?.runningJobId
    || (chatId && useExecutionStore.getState().hasActiveExecution(chatId))) {
    throw new Error('Stop the running session before deleting it.');
  }
  if (chatId) {
    const resumeWrites = tab ? await pauseBuilderSessionWrites(chatId) : () => {};
    try { await deleteSession(chatId); }
    catch (error) {
      // A canvas can exist before its first persisted conversation, or the
      // session may already have been deleted in another browser.
      if (!isAxiosError(error) || error.response?.status !== 404) { resumeWrites(); throw error; }
    }
    // Prune locally after server success. The legacy Chat delete action selects
    // the first server session, which may be archived or linked to a builder.
    const chatStore = useSessionStore.getState();
    if (chatStore.currentSessionId === chatId) {
      chatStore.startNewChat();
      if (useUILayoutStore.getState().appMode === 'chat') useExecutionStore.getState().resetForSession();
    }
    useSessionStore.setState(state => ({ sessions: state.sessions.filter(item => item.id !== chatId) }));
    useChatMessagesStore.getState().clearSession(chatId);
    sessionStorage.removeItem(`kasal-builder-run:${groupId || 'default'}:${chatId}`);
    useSessionPreferences.getState().remove(`chat:${chatId}`);
  }
  if (tab) {
    const wasActive = useBuilderCanvasStore.getState().activeCanvasId === tab.id;
    useBuilderCanvasStore.getState().closeCanvas(tab.id);
    const mode = useUILayoutStore.getState().appMode;
    if (wasActive && mode !== 'chat' && groupId === (localStorage.getItem('selectedGroupId') || '')) {
      switchWorkspaceMode(mode);
    }
  }
  useSessionPreferences.getState().remove(session.key);
}
