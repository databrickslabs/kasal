import { useMemo } from 'react';
import { useUILayoutStore } from '../../store/uiLayout';
import { useTabManagerStore } from '../../store/tabManager';
import { useSessionStore } from '../../features/chat/store/sessionStore';
import { useChatMessagesStore } from '../../features/workflow/assistant/store/chatMessagesStore';

/** Use explicit run links, never crew names: the same crew can run in many sessions. */
export function useSessionRunIds() {
  const mode = useUILayoutStore(state => state.appMode);
  const tab = useTabManagerStore(state => state.tabs.find(item => item.id === state.activeTabId));
  const chatId = useSessionStore(state => state.currentSessionId);
  const chatMessages = useSessionStore(state => state.messages);
  const runningJobId = useSessionStore(state => state.sessions.find(item => item.id === state.currentSessionId)?.runningJobId);
  const builderMessages = useChatMessagesStore(state => tab?.chatSessionId ? state.messagesBySession[tab.chatSessionId] : undefined);
  const ids = useMemo(() => {
    const linked = mode === 'chat'
      ? [...chatMessages.map(message => message.executionId), runningJobId]
      : [...(tab?.executionJobIds || []), ...(builderMessages || []).map(message => message.jobId)];
    return [...new Set(linked.filter((id): id is string => Boolean(id)))].sort();
  }, [mode, chatMessages, runningJobId, tab?.executionJobIds, builderMessages]);
  return { ids, sessionKey: mode === 'chat' ? `chat:${chatId}` : `builder:${tab?.id}` };
}
