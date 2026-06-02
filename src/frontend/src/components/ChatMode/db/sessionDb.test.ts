import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { ChatMessage } from '../types/chat';

// ---------------------------------------------------------------------------
// In-memory mock of the `idb` package (openDB / IDBPDatabase).
// Supports the subset used by sessionDb.ts:
//   - put, get, getAll, delete
//   - getAllFromIndex
//   - transaction('store','readwrite') -> { store: { index(name) }, done }
//   - index.openCursor(key) -> cursor with value, delete(), continue()
//   - objectStoreNames.contains(name) + createObjectStore + createIndex
// ---------------------------------------------------------------------------

interface StoreDef {
  keyPath: string;
  records: Map<unknown, Record<string, unknown>>;
  indexes: Map<string, string>; // indexName -> indexed field
}

class FakeCursor {
  constructor(
    private matches: Array<{ key: unknown; value: Record<string, unknown> }>,
    private idx: number,
    private store: StoreDef,
  ) {}

  get value() {
    return this.matches[this.idx].value;
  }

  async delete() {
    const { key } = this.matches[this.idx];
    this.store.records.delete(key);
  }

  async continue() {
    const next = this.idx + 1;
    if (next < this.matches.length) {
      return new FakeCursor(this.matches, next, this.store);
    }
    return null;
  }
}

class FakeIndex {
  constructor(private store: StoreDef, private field: string) {}

  async openCursor(key: unknown) {
    const matches: Array<{ key: unknown; value: Record<string, unknown> }> = [];
    for (const [recKey, value] of this.store.records.entries()) {
      if (value[this.field] === key) {
        matches.push({ key: recKey, value });
      }
    }
    if (matches.length === 0) {
      return null;
    }
    return new FakeCursor(matches, 0, this.store);
  }
}

class FakeUpgradeStore {
  constructor(private def: StoreDef) {}
  createIndex(name: string, field: string) {
    this.def.indexes.set(name, field);
  }
}

class FakeUpgradeDb {
  objectStoreNames: { contains: (name: string) => boolean };
  constructor(private stores: Map<string, StoreDef>) {
    this.objectStoreNames = {
      contains: (name: string) => this.stores.has(name),
    };
  }
  createObjectStore(name: string, opts: { keyPath: string }) {
    const def: StoreDef = {
      keyPath: opts.keyPath,
      records: new Map(),
      indexes: new Map(),
    };
    this.stores.set(name, def);
    return new FakeUpgradeStore(def);
  }
}

class FakeDb {
  constructor(public stores: Map<string, StoreDef>) {}

  private store(name: string): StoreDef {
    const s = this.stores.get(name);
    if (!s) throw new Error(`unknown store ${name}`);
    return s;
  }

  async put(storeName: string, value: Record<string, unknown>) {
    const s = this.store(storeName);
    s.records.set(value[s.keyPath], value);
  }

  async get(storeName: string, key: unknown) {
    return this.store(storeName).records.get(key);
  }

  async getAll(storeName: string) {
    return Array.from(this.store(storeName).records.values());
  }

  async delete(storeName: string, key: unknown) {
    this.store(storeName).records.delete(key);
  }

  async getAllFromIndex(storeName: string, indexName: string, key: unknown) {
    const s = this.store(storeName);
    const field = s.indexes.get(indexName);
    if (!field) throw new Error(`unknown index ${indexName}`);
    return Array.from(s.records.values()).filter((v) => v[field] === key);
  }

  transaction(storeName: string, _mode: string) {
    const def = this.store(storeName);
    return {
      store: {
        index: (name: string) => {
          const field = def.indexes.get(name);
          if (!field) throw new Error(`unknown index ${name}`);
          return new FakeIndex(def, field);
        },
      },
      done: Promise.resolve(),
    };
  }
}

// Holds the singleton DB the mock hands out so tests can reset/inspect it.
let mockStores: Map<string, StoreDef>;
const openDBMock = vi.fn(
  async (
    _name: string,
    _version: number,
    opts: { upgrade: (db: FakeUpgradeDb) => void },
  ) => {
    // Run the upgrade callback against a fresh upgrade view of the stores.
    opts.upgrade(new FakeUpgradeDb(mockStores));
    return new FakeDb(mockStores) as unknown as import('idb').IDBPDatabase;
  },
);

vi.mock('idb', () => ({
  openDB: (...args: unknown[]) =>
    (openDBMock as unknown as (...a: unknown[]) => unknown)(...args),
}));

// Import AFTER mocks are registered. Because initDb caches a module-level
// singleton promise, we re-import a fresh module copy in beforeEach via
// vi.resetModules() so each test starts clean.
let sessionDb: typeof import('./sessionDb');

beforeEach(async () => {
  mockStores = new Map();
  openDBMock.mockClear();
  vi.resetModules();
  sessionDb = await import('./sessionDb');
});

const makeMsg = (overrides: Partial<ChatMessage> = {}): ChatMessage => ({
  id: 'm1',
  role: 'user',
  content: 'hello',
  timestamp: new Date('2023-01-01T00:00:00Z'),
  ...overrides,
});

describe('initDb', () => {
  it('creates all three object stores on upgrade and caches the promise', async () => {
    const a = await sessionDb.initDb();
    const b = await sessionDb.initDb();

    // Singleton: openDB only called once and same db returned.
    expect(openDBMock).toHaveBeenCalledTimes(1);
    expect(a).toBe(b);

    expect(mockStores.has('sessions')).toBe(true);
    expect(mockStores.has('messages')).toBe(true);
    expect(mockStores.has('previews')).toBe(true);
    // messages store has the by-session index created.
    expect(mockStores.get('messages')!.indexes.get('by-session')).toBe(
      'sessionId',
    );
  });

  it('upgrade is a no-op when stores already exist', async () => {
    // Pre-populate the stores so contains() returns true for all three.
    mockStores.set('sessions', {
      keyPath: 'id',
      records: new Map(),
      indexes: new Map(),
    });
    mockStores.set('messages', {
      keyPath: 'id',
      records: new Map(),
      indexes: new Map([['by-session', 'sessionId']]),
    });
    mockStores.set('previews', {
      keyPath: 'sessionId',
      records: new Map(),
      indexes: new Map(),
    });

    await sessionDb.initDb();
    expect(openDBMock).toHaveBeenCalledTimes(1);
    // Index map untouched / not recreated.
    expect(mockStores.get('messages')!.indexes.size).toBe(1);
  });
});

describe('createSession', () => {
  it('creates and stores a session with generated id and timestamps', async () => {
    const session = await sessionDb.createSession('My title');
    expect(session.title).toBe('My title');
    expect(session.id).toMatch(/^session-\d+-[a-z0-9]+$/);
    expect(session.createdAt).toBeInstanceOf(Date);
    expect(session.updatedAt).toBeInstanceOf(Date);

    const stored = mockStores.get('sessions')!.records.get(session.id);
    expect(stored).toMatchObject({ id: session.id, title: 'My title' });
  });
});

describe('listSessions', () => {
  it('coerces dates and sorts by updatedAt descending', async () => {
    const sessions = mockStores.get('sessions');
    // initDb hasn't run yet; trigger it to create stores.
    await sessionDb.initDb();

    const store = mockStores.get('sessions')!;
    store.records.set('older', {
      id: 'older',
      title: 'older',
      createdAt: '2023-01-01T00:00:00Z',
      updatedAt: '2023-01-01T00:00:00Z',
    });
    store.records.set('newer', {
      id: 'newer',
      title: 'newer',
      createdAt: '2023-06-01T00:00:00Z',
      updatedAt: '2023-06-01T00:00:00Z',
    });

    const result = await sessionDb.listSessions();
    expect(result.map((s) => s.id)).toEqual(['newer', 'older']);
    expect(result[0].createdAt).toBeInstanceOf(Date);
    expect(result[0].updatedAt).toBeInstanceOf(Date);
    expect(sessions).toBeUndefined(); // sanity: captured before init
  });

  it('returns empty list when no sessions', async () => {
    const result = await sessionDb.listSessions();
    expect(result).toEqual([]);
  });
});

describe('deleteSession', () => {
  it('removes session, preview, and all matching messages via cursor', async () => {
    await sessionDb.initDb();
    const sessions = mockStores.get('sessions')!;
    const previews = mockStores.get('previews')!;
    const messages = mockStores.get('messages')!;

    sessions.records.set('s1', { id: 's1', title: 't' });
    sessions.records.set('s2', { id: 's2', title: 't2' });
    previews.records.set('s1', { sessionId: 's1', type: 'x', data: 'd' });
    messages.records.set('m1', { id: 'm1', sessionId: 's1', content: 'a' });
    messages.records.set('m2', { id: 'm2', sessionId: 's1', content: 'b' });
    messages.records.set('m3', { id: 'm3', sessionId: 's2', content: 'c' });

    await sessionDb.deleteSession('s1');

    expect(sessions.records.has('s1')).toBe(false);
    expect(sessions.records.has('s2')).toBe(true);
    expect(previews.records.has('s1')).toBe(false);
    expect(messages.records.has('m1')).toBe(false);
    expect(messages.records.has('m2')).toBe(false);
    expect(messages.records.has('m3')).toBe(true);
  });

  it('handles delete when no messages match (cursor null)', async () => {
    await sessionDb.initDb();
    mockStores.get('sessions')!.records.set('s1', { id: 's1' });
    await expect(sessionDb.deleteSession('s1')).resolves.toBeUndefined();
    expect(mockStores.get('sessions')!.records.has('s1')).toBe(false);
  });
});

describe('renameSession', () => {
  it('renames an existing session and bumps updatedAt', async () => {
    await sessionDb.initDb();
    const store = mockStores.get('sessions')!;
    const old = new Date('2020-01-01T00:00:00Z');
    store.records.set('s1', {
      id: 's1',
      title: 'old',
      createdAt: old,
      updatedAt: old,
    });

    await sessionDb.renameSession('s1', 'new title');
    const updated = store.records.get('s1')!;
    expect(updated.title).toBe('new title');
    expect((updated.updatedAt as Date).getTime()).toBeGreaterThan(
      old.getTime(),
    );
  });

  it('does nothing when session not found', async () => {
    await sessionDb.initDb();
    await expect(
      sessionDb.renameSession('missing', 'x'),
    ).resolves.toBeUndefined();
    expect(mockStores.get('sessions')!.records.has('missing')).toBe(false);
  });
});

describe('touchSession', () => {
  it('updates updatedAt for existing session', async () => {
    await sessionDb.initDb();
    const store = mockStores.get('sessions')!;
    const old = new Date('2020-01-01T00:00:00Z');
    store.records.set('s1', {
      id: 's1',
      title: 't',
      createdAt: old,
      updatedAt: old,
    });

    await sessionDb.touchSession('s1');
    expect(
      (store.records.get('s1')!.updatedAt as Date).getTime(),
    ).toBeGreaterThan(old.getTime());
  });

  it('does nothing when session not found', async () => {
    await sessionDb.initDb();
    await expect(sessionDb.touchSession('missing')).resolves.toBeUndefined();
  });
});

describe('getSessionMessages', () => {
  it('returns messages with coerced timestamps', async () => {
    await sessionDb.initDb();
    const messages = mockStores.get('messages')!;
    messages.records.set('m1', {
      id: 'm1',
      sessionId: 's1',
      content: 'a',
      timestamp: '2023-01-01T00:00:00Z',
    });
    messages.records.set('m2', {
      id: 'm2',
      sessionId: 's2',
      content: 'b',
      timestamp: '2023-02-01T00:00:00Z',
    });

    const result = await sessionDb.getSessionMessages('s1');
    expect(result).toHaveLength(1);
    expect(result[0].id).toBe('m1');
    expect(result[0].timestamp).toBeInstanceOf(Date);
  });
});

describe('addMessageToSession', () => {
  it('stores message with sessionId and touches the session', async () => {
    await sessionDb.initDb();
    const sessions = mockStores.get('sessions')!;
    const old = new Date('2020-01-01T00:00:00Z');
    sessions.records.set('s1', {
      id: 's1',
      title: 't',
      createdAt: old,
      updatedAt: old,
    });

    await sessionDb.addMessageToSession('s1', makeMsg({ id: 'mx' }));

    const stored = mockStores.get('messages')!.records.get('mx');
    expect(stored).toMatchObject({ id: 'mx', sessionId: 's1' });
    // touchSession ran and bumped updatedAt.
    expect(
      (sessions.records.get('s1')!.updatedAt as Date).getTime(),
    ).toBeGreaterThan(old.getTime());
  });
});

describe('updateMessageInSession', () => {
  it('merges updates into existing message', async () => {
    await sessionDb.initDb();
    const messages = mockStores.get('messages')!;
    messages.records.set('m1', {
      id: 'm1',
      sessionId: 's1',
      content: 'old',
      role: 'user',
    });

    await sessionDb.updateMessageInSession('s1', 'm1', { content: 'new' });

    expect(messages.records.get('m1')).toMatchObject({
      id: 'm1',
      sessionId: 's1',
      content: 'new',
      role: 'user',
    });
  });

  it('does nothing when message not found', async () => {
    await sessionDb.initDb();
    await expect(
      sessionDb.updateMessageInSession('s1', 'missing', { content: 'x' }),
    ).resolves.toBeUndefined();
    expect(mockStores.get('messages')!.records.has('missing')).toBe(false);
  });
});

describe('clearSessionMessages', () => {
  it('deletes all messages for a session via cursor', async () => {
    await sessionDb.initDb();
    const messages = mockStores.get('messages')!;
    messages.records.set('m1', { id: 'm1', sessionId: 's1' });
    messages.records.set('m2', { id: 'm2', sessionId: 's1' });
    messages.records.set('m3', { id: 'm3', sessionId: 's2' });

    await sessionDb.clearSessionMessages('s1');

    expect(messages.records.has('m1')).toBe(false);
    expect(messages.records.has('m2')).toBe(false);
    expect(messages.records.has('m3')).toBe(true);
  });

  it('handles no matching messages (cursor null)', async () => {
    await sessionDb.initDb();
    await expect(
      sessionDb.clearSessionMessages('none'),
    ).resolves.toBeUndefined();
  });
});

describe('preview persistence', () => {
  it('saves, gets, and deletes a session preview', async () => {
    await sessionDb.initDb();

    await sessionDb.saveSessionPreview('s1', {
      type: 'html',
      data: '<p>hi</p>',
      title: 'Preview',
    });
    let stored = await sessionDb.getSessionPreview('s1');
    expect(stored).toMatchObject({
      sessionId: 's1',
      type: 'html',
      data: '<p>hi</p>',
      title: 'Preview',
    });

    await sessionDb.deleteSessionPreview('s1');
    stored = await sessionDb.getSessionPreview('s1');
    expect(stored).toBeUndefined();
  });

  it('getSessionPreview returns undefined when absent', async () => {
    await sessionDb.initDb();
    expect(await sessionDb.getSessionPreview('nope')).toBeUndefined();
  });
});
