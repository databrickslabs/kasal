import { openDB, deleteDB, IDBPDatabase } from 'idb';
import { ChatMessage, ChatSession } from '../types/chat';

const DB_NAME = 'kasal-chat-db';
// Preserve the legacy schema version while reading sessions for server migration.
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

// Legacy job markers must stay out of migrated session lists.
const RUNNING_JOB_PREFIX = 'running-job:';

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
    // Exclude running-job marker records (reserved id prefix) — they aren't
    // real sessions and would otherwise show as phantom rows in the history rail.
    .filter((s) => !(typeof s.id === 'string' && s.id.startsWith(RUNNING_JOB_PREFIX)))
    .filter((s) => !groupId || s.groupId === groupId)
    .map((s) => ({
      ...s,
      createdAt: new Date(s.createdAt),
      updatedAt: new Date(s.updatedAt),
    }))
    .sort((a, b) => b.updatedAt.getTime() - a.updatedAt.getTime());
}

export async function getSessionMessages(sessionId: string): Promise<ChatMessage[]> {
  const db = await initDb();
  const all = await db.getAllFromIndex('messages', 'by-session', sessionId);
  return all.map((m) => ({
    ...m,
    timestamp: new Date(m.timestamp),
  }));
}
