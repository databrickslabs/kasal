import type { BuilderCanvas } from './builderCanvasStore';
import type { ChatSession } from '../../features/chat/types/chat';
import type { AppMode } from '../../store/uiLayout';

export interface WorkspaceSession {
  key: string;
  id: string;
  title: string;
  mode: AppMode;
  updatedAt: number;
  running: boolean;
}

/** Only newly-created, empty drafts are hidden. Legacy canvases retain their history. */
export function isEmptySessionDraft(tab: BuilderCanvas) {
  return Boolean(tab.isSessionDraft && !tab.nodes.length && !tab.flowNodes.length && !tab.savedCrewId && !tab.savedFlowId);
}

/** Keep the existing IDs: a builder's conversation must never become a second Chat row. */
export function collectSessions(
  chats: ChatSession[], canvases: BuilderCanvas[], groupId: string,
  permissions: { crew: boolean; flow: boolean },
): WorkspaceSession[] {
  const builderChatIds = new Set(canvases.map(tab => tab.chatSessionId));
  return [
    ...chats.filter(chat => (!chat.mode || chat.mode === 'chat') && (!chat.groupId || chat.groupId === groupId) && !builderChatIds.has(chat.id)).map(chat => ({
      key: `chat:${chat.id}`, id: chat.id, title: chat.title, mode: 'chat' as const,
      updatedAt: new Date(chat.updatedAt).getTime(), running: Boolean(chat.runningJobId),
    })),
    ...canvases.filter(tab => tab.group_id === groupId && permissions[tab.viewMode] && !isEmptySessionDraft(tab)).map(tab => ({
      key: `builder:${tab.id}`, id: tab.id, title: tab.name, mode: tab.viewMode,
      updatedAt: new Date(tab.lastModified).getTime(), running: tab.executionStatus === 'running',
    })),
  ].sort((a, b) => b.updatedAt - a.updatedAt || a.key.localeCompare(b.key));
}
