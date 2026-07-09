/**
 * Server-side chat session storage for the chat-mode workspace.
 *
 * Drop-in replacement for the IndexedDB adapter (sessionDb.ts): same function
 * contract, but sessions and messages persist through the backend
 * /chat-history API. The backend stores them via its smart-routed DB session,
 * so they land in SQLite during local dev and in Lakebase when a Lakebase
 * backend is active — and survive browser/profile changes.
 *
 * Previews and the running-job (refresh-reconnect) marker also live server-side
 * now (on the chat_sessions row). They used to sit in browser IndexedDB, which
 * meant they didn't follow the user across browsers AND silently broke once
 * sessions themselves moved to the server.
 */

import { ChatMessage, ChatSession } from '../types/chat';
import { getClient } from '../api/client';
import {
  initDb as initLocalDb,
  listSessions as listLocalSessions,
  getSessionMessages as getLocalSessionMessages,
} from './sessionDb';

const BASE = '/chat-history';

// ---------------------------------------------------------------------------
// Wire types (backend schemas)
// ---------------------------------------------------------------------------

interface NamedSessionWire {
  id: string;
  title: string;
  user_id: string;
  group_id?: string | null;
  created_at: string;
  updated_at: string;
}

interface MessageWire {
  id: string;
  session_id: string;
  message_type: string;
  content: string;
  intent?: string | null;
  generation_result?: Record<string, unknown> | null;
  timestamp: string;
}

/** Backend timestamps are naive UTC — parse them as UTC, not local time. */
const parseUtc = (iso: string): Date =>
  new Date(/[zZ]|[+-]\d{2}:?\d{2}$/.test(iso) ? iso : `${iso}Z`);

const toSession = (w: NamedSessionWire): ChatSession => ({
  id: w.id,
  title: w.title,
  createdAt: parseUtc(w.created_at),
  updatedAt: parseUtc(w.updated_at),
  ...(w.group_id ? { groupId: w.group_id } : {}),
});

// ChatMode-specific message fields ride in generation_result under this key
// so the column stays compatible with the sidebar chat's usage.
const EXTRA_KEY = '__chatmode';

interface ChatModeExtras {
  resultType?: string;
  resultData?: unknown;
  attachments?: string[];
  executionId?: string;
  usedWorkspaceMemory?: boolean;
}

/** True when a stored resultData is (or wraps) an A2UI surface — used to heal
 *  messages whose `__chatmode` envelope was clobbered by an old partial update
 *  (only `resultData` survived): they must still render as an a2ui card. */
const looksLikeSurface = (v: unknown): boolean => {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  if (typeof o.surfaceKind === 'string' && Array.isArray(o.components)) return true;
  return Boolean(o.a2ui && typeof o.a2ui === 'object');
};

const toMessage = (w: MessageWire): ChatMessage => {
  const extras = (w.generation_result?.[EXTRA_KEY] || {}) as ChatModeExtras;
  const role: ChatMessage['role'] =
    w.message_type === 'user' || w.message_type === 'system'
      ? (w.message_type as ChatMessage['role'])
      : 'assistant';
  // Heal envelope-clobbered rows: a surface-shaped resultData without a
  // resultType is an a2ui card whose type was stripped by a partial PUT.
  const resultType =
    extras.resultType ?? (looksLikeSurface(extras.resultData) ? 'a2ui' : undefined);
  return {
    id: w.id,
    sessionId: w.session_id,
    role,
    content: w.content === '[ui-card]' ? '' : w.content,
    timestamp: parseUtc(w.timestamp),
    ...(w.intent ? { intent: w.intent as ChatMessage['intent'] } : {}),
    ...(resultType ? { resultType } : {}),
    ...(extras.resultData !== undefined ? { resultData: extras.resultData } : {}),
    ...(extras.attachments ? { attachments: extras.attachments } : {}),
    ...(extras.executionId ? { executionId: extras.executionId } : {}),
    ...(extras.usedWorkspaceMemory !== undefined
      ? { usedWorkspaceMemory: extras.usedWorkspaceMemory }
      : {}),
    isStreaming: false,
  };
};

const packExtras = (msg: Partial<ChatMessage>): Record<string, unknown> | undefined => {
  const extras: ChatModeExtras = {};
  if (msg.resultType !== undefined) extras.resultType = msg.resultType;
  if (msg.resultData !== undefined) extras.resultData = msg.resultData;
  if (msg.attachments !== undefined) extras.attachments = msg.attachments;
  if (msg.executionId !== undefined) extras.executionId = msg.executionId;
  if (msg.usedWorkspaceMemory !== undefined) extras.usedWorkspaceMemory = msg.usedWorkspaceMemory;
  return Object.keys(extras).length > 0 ? { [EXTRA_KEY]: extras } : undefined;
};

// ---------------------------------------------------------------------------
// Adapter contract (mirrors sessionDb.ts)
// ---------------------------------------------------------------------------

export async function initDb(): Promise<void> {
  // Sessions live server-side; nothing to open. Kick off the one-time
  // IndexedDB migration in the background so old local chats show up.
  void migrateLocalSessionsToServer().catch(() => {
    /* best-effort; retried next init */
  });
}

/** Server sessions are always workspace-tagged — nothing to adopt. */
export async function assignUngroupedSessions(_groupId: string): Promise<void> {
  return;
}

export async function createSession(title: string, _groupId?: string): Promise<ChatSession> {
  const res = await getClient().post<NamedSessionWire>(`${BASE}/sessions`, { title });
  return toSession(res.data);
}

/** Group scoping is enforced by the backend from the request's group header. */
export async function listSessions(_groupId?: string): Promise<ChatSession[]> {
  const res = await getClient().get<NamedSessionWire[]>(`${BASE}/sessions/named`);
  return (res.data || []).map(toSession);
}

export async function deleteSession(id: string): Promise<void> {
  await getClient().delete(`${BASE}/sessions/${id}`);
}

export async function renameSession(id: string, title: string): Promise<void> {
  await getClient().put(`${BASE}/sessions/${id}`, { title });
}

export async function getSessionMessages(sessionId: string): Promise<ChatMessage[]> {
  const res = await getClient().get<{ messages: MessageWire[] }>(
    `${BASE}/sessions/${sessionId}/messages`,
    { params: { page: 0, per_page: 100 } },
  );
  return (res.data?.messages || []).map(toMessage);
}

// Card-only messages (crew cards, prompts) carry no text — the backend
// requires non-empty content, so they persist with this sentinel and it is
// stripped again on load.
const CARD_PLACEHOLDER = '[ui-card]';

export async function addMessageToSession(
  sessionId: string,
  msg: ChatMessage,
): Promise<void> {
  const hasCard = Boolean(msg.resultType || msg.resultData !== undefined);
  if (!msg.content && !hasCard) return; // nothing to persist
  await getClient().post(`${BASE}/messages`, {
    id: msg.id,
    session_id: sessionId,
    message_type: msg.role,
    content: msg.content || CARD_PLACEHOLDER,
    intent: msg.intent ?? null,
    generation_result: packExtras(msg) ?? null,
  });
}

export async function updateMessageInSession(
  _sessionId: string,
  msgId: string,
  updates: Partial<ChatMessage>,
): Promise<void> {
  const payload: Record<string, unknown> = {};
  if (updates.content) payload.content = updates.content;
  if (updates.intent) payload.intent = updates.intent;
  const extras = packExtras(updates);
  if (extras) payload.generation_result = extras;
  // isStreaming flips and other transient-only updates need no round trip
  if (Object.keys(payload).length === 0) return;
  await getClient().put(`${BASE}/messages/${msgId}`, payload);
}

/** Clearing keeps the session but drops its messages: delete + recreate id. */
export async function clearSessionMessages(sessionId: string): Promise<void> {
  // The delete endpoint removes messages AND the named-session row, so
  // recreate the row immediately with the same id to keep the session.
  const client = getClient();
  await client.delete(`${BASE}/sessions/${sessionId}`).catch(() => undefined);
  await client
    .post(`${BASE}/sessions`, { id: sessionId, title: 'New Chat' })
    .catch(() => undefined);
}

// ---------------------------------------------------------------------------
// Per-session preview (rendered A2UI deliverable). Server-side replacement for
// the IndexedDB 'previews' store. Reads are resilient (return undefined on any
// error) so a transient failure never blocks the chat; writes are best-effort.
// ---------------------------------------------------------------------------

export interface StoredPreview {
  sessionId: string;
  type: string;
  data: string;
  title?: string;
}

interface PreviewWire {
  type: string | null;
  data: string | null;
  title: string | null;
}

export async function saveSessionPreview(
  sessionId: string,
  preview: { type: string; data: string; title?: string },
): Promise<void> {
  try {
    await getClient().put(`${BASE}/sessions/${sessionId}/preview`, {
      type: preview.type,
      data: preview.data,
      title: preview.title ?? null,
    });
  } catch {
    /* best-effort persistence — never break the chat on a failed write */
  }
}

export async function getSessionPreview(
  sessionId: string,
): Promise<StoredPreview | undefined> {
  try {
    const res = await getClient().get<PreviewWire>(`${BASE}/sessions/${sessionId}/preview`);
    const w = res.data;
    if (!w || !w.data) return undefined; // no preview stored
    return {
      sessionId,
      type: w.type || 'ui',
      data: w.data,
      ...(w.title ? { title: w.title } : {}),
    };
  } catch {
    return undefined;
  }
}

export async function deleteSessionPreview(sessionId: string): Promise<void> {
  try {
    await getClient().delete(`${BASE}/sessions/${sessionId}/preview`);
  } catch {
    /* best-effort */
  }
}

// ---------------------------------------------------------------------------
// Per-session in-flight crew job marker (refresh reconnect). Server-side
// replacement for the IndexedDB marker.
// ---------------------------------------------------------------------------

export async function setSessionRunningJob(sessionId: string, jobId: string): Promise<void> {
  try {
    await getClient().put(`${BASE}/sessions/${sessionId}/running-job`, { job_id: jobId });
  } catch {
    /* best-effort — reconnect just won't happen if this write is lost */
  }
}

export async function getSessionRunningJob(sessionId: string): Promise<string | null> {
  try {
    const res = await getClient().get<{ job_id: string | null }>(
      `${BASE}/sessions/${sessionId}/running-job`,
    );
    return res.data?.job_id ?? null;
  } catch {
    return null;
  }
}

export async function clearSessionRunningJob(sessionId: string): Promise<void> {
  try {
    await getClient().delete(`${BASE}/sessions/${sessionId}/running-job`);
  } catch {
    /* best-effort */
  }
}

// ---------------------------------------------------------------------------
// One-time migration: move existing IndexedDB sessions to the server so chat
// history survives the storage switch. Per-session idempotent — each migrated
// session id is recorded locally and skipped on the next run.
// ---------------------------------------------------------------------------

const MIGRATED_KEY = 'kasal-chat-sessions-migrated-ids';

const migratedIds = (): Set<string> => {
  try {
    return new Set(JSON.parse(localStorage.getItem(MIGRATED_KEY) || '[]'));
  } catch {
    return new Set();
  }
};

const markMigrated = (ids: Set<string>): void => {
  try {
    localStorage.setItem(MIGRATED_KEY, JSON.stringify([...ids]));
  } catch {
    /* storage full/blocked — migration will re-check against the server */
  }
};

export async function migrateLocalSessionsToServer(): Promise<number> {
  await initLocalDb();
  const groupId = localStorage.getItem('selectedGroupId') || undefined;
  // Only migrate sessions belonging to the CURRENT workspace (plus untagged
  // pre-scoping ones) — the server tags rows with the request's group header,
  // so migrating another workspace's sessions here would mis-tag them.
  const local = (await listLocalSessions()).filter(
    (s) => !s.groupId || !groupId || s.groupId === groupId,
  );
  if (local.length === 0) return 0;

  const done = migratedIds();
  // Sessions already on the server never need migrating (covers a wiped
  // localStorage flag and avoids id-conflict failures).
  try {
    (await listSessions()).forEach((s) => done.add(s.id));
  } catch {
    return 0; // server unreachable — retry next init
  }
  let migrated = 0;
  for (const session of local) {
    if (done.has(session.id)) continue;
    try {
      await getClient().post(`${BASE}/sessions`, {
        id: session.id,
        title: session.title || 'New Chat',
      });
      const messages = await getLocalSessionMessages(session.id);
      for (const msg of messages) {
        await addMessageToSession(session.id, msg);
      }
      done.add(session.id);
      markMigrated(done);
      migrated += 1;
    } catch {
      // Server may already have it (id conflict) or be unreachable —
      // mark id-conflicts as done on the next listSessions reconciliation.
      break;
    }
  }
  return migrated;
}
