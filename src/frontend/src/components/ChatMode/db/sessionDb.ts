import { openDB, IDBPDatabase } from 'idb';
import { ChatMessage, ChatSession } from '../types/chat';

const DB_NAME = 'kasal-chat-db';
const DB_VERSION = 2;

let dbPromise: Promise<IDBPDatabase> | null = null;

export function initDb(): Promise<IDBPDatabase> {
  if (!dbPromise) {
    dbPromise = openDB(DB_NAME, DB_VERSION, {
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
    });
  }
  return dbPromise;
}

export async function createSession(title: string): Promise<ChatSession> {
  const db = await initDb();
  const now = new Date();
  const session: ChatSession = {
    id: `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    title,
    createdAt: now,
    updatedAt: now,
  };
  await db.put('sessions', session);
  return session;
}

export async function listSessions(): Promise<ChatSession[]> {
  const db = await initDb();
  const all = await db.getAll('sessions');
  // Sort by updatedAt descending
  return all
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
