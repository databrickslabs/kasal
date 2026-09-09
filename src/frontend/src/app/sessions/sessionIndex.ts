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
  canvasPending?: boolean;
}

/** Only newly-created, empty drafts are hidden. Legacy canvases retain their history. */
export function isEmptySessionDraft(tab: BuilderCanvas) {
  return Boolean(tab.isSessionDraft && !tab.nodes.length && !tab.flowNodes.length && !tab.savedCrewId && !tab.savedFlowId);
}

/** The conversation ID is available before the canvas payload has loaded. */
export const builderSessionKey = (tab: BuilderCanvas) => `builder:${tab.chatSessionId || tab.id}`;

export function collectSessions(
  chats: ChatSession[], canvases: BuilderCanvas[], groupId: string,
  permissions: { crew: boolean; flow: boolean }, unavailableSessionIds: string[] = [],
): WorkspaceSession[] {
  const scoped = canvases.filter(tab => tab.group_id === groupId);
  const bySession = new Map(scoped.map(tab => [tab.chatSessionId || tab.id, tab]));
  const metadata = chats.filter(chat => !chat.groupId || chat.groupId === groupId);
  const indexed = new Set(metadata.map(chat => chat.id));
  const rows: WorkspaceSession[] = [];
  for (const chat of metadata) {
    const tab = bySession.get(chat.id);
    const mode = tab?.viewMode || chat.mode || 'chat';
    if (mode !== 'chat' && !permissions[mode]) continue;
    rows.push({
      key: mode === 'chat' ? `chat:${chat.id}` : `builder:${chat.id}`,
      id: tab?.id || chat.id, title: tab?.name || chat.title, mode,
      // Hydration must not change the server list's ordering timestamp.
      updatedAt: new Date(chat.updatedAt).getTime(),
      running: Boolean(chat.runningJobId) || tab?.executionStatus === 'running',
      ...(mode !== 'chat' ? { canvasPending: !tab || unavailableSessionIds.includes(chat.id) } : {}),
    });
  }
  for (const tab of scoped) {
    if (indexed.has(tab.chatSessionId || tab.id) || !permissions[tab.viewMode] || isEmptySessionDraft(tab)) continue;
    rows.push({
      key: builderSessionKey(tab), id: tab.id, title: tab.name, mode: tab.viewMode,
      updatedAt: new Date(tab.lastModified).getTime(), running: tab.executionStatus === 'running',
      canvasPending: unavailableSessionIds.includes(tab.chatSessionId || tab.id),
    });
  }
  return rows.sort((a, b) => b.updatedAt - a.updatedAt || a.key.localeCompare(b.key));
}
