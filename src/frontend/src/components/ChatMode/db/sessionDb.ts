import { openDB, deleteDB, IDBPDatabase } from 'idb';
import { ChatMessage, ChatSession } from '../types/chat';

const DB_NAME = 'kasal-chat-db';
// Keep at 2 — the original schema. The in-flight-job marker is stored on the
// existing `sessions` record (see setSessionRunningJob) rather than a new store,
// so we never bump the version. (A version bump can BLOCK the upgrade when
// another tab holds an older connection, hanging every read/write — that's what
// broke session persistence + the New Chat button.)
const DB_VERSION = 2;

let dbPromise: Promise<IDBPDatabase> | null = null;

const COMMON_OPTS = {
  blocked() {
    // eslint-disable-next-line no-console
    console.warn('[sessionDb] DB open blocked by another tab. Close other Kasal tabs.');
  },
  blocking() {
    dbPromise?.then((db) => db.close()).catch(() => {});
    dbPromise = null;
  },
  terminated() {
    dbPromise = null;
  },
};

function openVersioned(): Promise<IDBPDatabase> {
  return openDB(DB_NAME, DB_VERSION, {
    upgrade(db) {
      if (!db.objectStoreNames.contains('sessions')) {
        db.createObjectStore('sessions', { keyPath: 'id' });
      }
      if (!db.objectStoreNames.contains('messages')) {
        const msgStore = db.createObjectStore('messages', { keyPath: 'id' });
        msgStore.createIndex('by-session', 'sessionId');
      }
      if (!db.objectStoreNames.contains('previews')) {
        db.createObjectStore('previews', { keyPath: 'sessionId' });
      }
    },
    ...COMMON_OPTS,
  });
}

export function initDb(): Promise<IDBPDatabase> {
  if (!dbPromise) {
    dbPromise = openVersioned().catch(async (err) => {
      // A DB left at a HIGHER version by a previous (newer-schema) build cannot
      // be opened at this version — it fails fast with a VersionError. Reopen
      // at the DB's CURRENT version (no upgrade, no block) so the user's
      // sessions/messages are preserved; the stores we need already exist.
      // eslint-disable-next-line no-console
      console.warn('[sessionDb] versioned open failed; reopening at current version', err);
      try {
        return await openDB(DB_NAME, undefined, COMMON_OPTS);
      } catch (err2) {
        // Last resort — DB is unusable; recreate it clean so the chat works.
        // eslint-disable-next-line no-console
        console.warn('[sessionDb] reopen failed; resetting chat DB', err2);
        await deleteDB(DB_NAME).catch(() => {});
        return openVersioned();
      }
    });
  }
  return dbPromise;
}

// --- In-flight crew run marker (for refresh reconnect) ---------------------
// Stored on the session record itself (no extra object store / version bump).

/** Record the in-flight crew job for a session. */
export async function setSessionRunningJob(sessionId: string, jobId: string): Promise<void> {
  const db = await initDb();
  const session = await db.get('sessions', sessionId);
  if (session) {
    await db.put('sessions', { ...session, runningJobId: jobId });
  }
}

/** Read the in-flight crew job for a session, or null if none. */
export async function getSessionRunningJob(sessionId: string): Promise<string | null> {
  const db = await initDb();
  const session = await db.get('sessions', sessionId);
  return session?.runningJobId ?? null;
}

/** Clear the in-flight crew job marker for a session (run finished/stopped). */
export async function clearSessionRunningJob(sessionId: string): Promise<void> {
  const db = await initDb();
  const session = await db.get('sessions', sessionId);
  if (session && 'runningJobId' in session) {
    delete session.runningJobId;
    await db.put('sessions', session);
  }
}

/**
 * One-time migration: claim sessions that predate workspace-scoping (no
 * groupId) for the given workspace, so strict per-workspace filtering doesn't
 * make them vanish. Idempotent — once everything is tagged it's a no-op.
 */
export async function assignUngroupedSessions(groupId: string): Promise<void> {
  if (!groupId) return;
  const db = await initDb();
  const all = await db.getAll('sessions');
  const ungrouped = all.filter((s) => !s.groupId);
  if (ungrouped.length === 0) return;
  const tx = db.transaction('sessions', 'readwrite');
  for (const s of ungrouped) {
    await tx.store.put({ ...s, groupId });
  }
  await tx.done;
}

export async function createSession(title: string, groupId?: string): Promise<ChatSession> {
  const db = await initDb();
  const now = new Date();
  const session: ChatSession = {
    id: `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    title,
    createdAt: now,
    updatedAt: now,
    ...(groupId ? { groupId } : {}),
  };
  await db.put('sessions', session);
  return session;
}

/**
 * List chat sessions, newest first. When ``groupId`` is given, ONLY sessions
 * belonging to that workspace are returned — strict per-workspace isolation, so
 * switching workspace never shows another workspace's chats. Sessions created
 * before workspace-scoping (no ``groupId``) are treated as not belonging to any
 * workspace and are hidden once a workspace is selected.
 */
export async function listSessions(groupId?: string): Promise<ChatSession[]> {
  const db = await initDb();
  const all = await db.getAll('sessions');
  return all
    .filter((s) => !groupId || s.groupId === groupId)
    .map((s) => ({
      ...s,
      createdAt: new Date(s.createdAt),
      updatedAt: new Date(s.updatedAt),
    }))
    .sort((a, b) => b.updatedAt.getTime() - a.updatedAt.getTime());
}

export async function deleteSession(id: string): Promise<void> {
  const db = await initDb();
  await db.delete('sessions', id);
  // Delete preview for this session
  await db.delete('previews', id);
  // Delete all messages for this session
  const tx = db.transaction('messages', 'readwrite');
  const index = tx.store.index('by-session');
  let cursor = await index.openCursor(id);
  while (cursor) {
    await cursor.delete();
    cursor = await cursor.continue();
  }
  await tx.done;
}

export async function renameSession(id: string, title: string): Promise<void> {
  const db = await initDb();
  const session = await db.get('sessions', id);
  if (session) {
    session.title = title;
    session.updatedAt = new Date();
    await db.put('sessions', session);
  }
}

export async function touchSession(id: string): Promise<void> {
  const db = await initDb();
  const session = await db.get('sessions', id);
  if (session) {
    session.updatedAt = new Date();
    await db.put('sessions', session);
  }
}

export async function getSessionMessages(sessionId: string): Promise<ChatMessage[]> {
  const db = await initDb();
  const all = await db.getAllFromIndex('messages', 'by-session', sessionId);
  return all.map((m) => ({
    ...m,
    timestamp: new Date(m.timestamp),
  }));
}

export async function addMessageToSession(
  sessionId: string,
  msg: ChatMessage
): Promise<void> {
  const db = await initDb();
  await db.put('messages', { ...msg, sessionId });
  await touchSession(sessionId);
}

export async function updateMessageInSession(
  sessionId: string,
  msgId: string,
  updates: Partial<ChatMessage>
): Promise<void> {
  const db = await initDb();
  const existing = await db.get('messages', msgId);
  if (existing) {
    const updated = { ...existing, ...updates, sessionId };
    await db.put('messages', updated);
  }
}

export async function clearSessionMessages(sessionId: string): Promise<void> {
  const db = await initDb();
  const tx = db.transaction('messages', 'readwrite');
  const index = tx.store.index('by-session');
  let cursor = await index.openCursor(sessionId);
  while (cursor) {
    await cursor.delete();
    cursor = await cursor.continue();
  }
  await tx.done;
}

// --- Preview persistence ---

export interface StoredPreview {
  sessionId: string;
  type: string;
  data: string;
  title?: string;
}

export async function saveSessionPreview(
  sessionId: string,
  preview: { type: string; data: string; title?: string },
): Promise<void> {
  const db = await initDb();
  await db.put('previews', { sessionId, ...preview });
}

export async function getSessionPreview(
  sessionId: string,
): Promise<StoredPreview | undefined> {
  const db = await initDb();
  return db.get('previews', sessionId);
}

export async function deleteSessionPreview(sessionId: string): Promise<void> {
  const db = await initDb();
  await db.delete('previews', sessionId);
}
