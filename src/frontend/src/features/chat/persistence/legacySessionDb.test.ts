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

  close() {
    // no-op (exercised by the `blocking` callback)
  }

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
        put: async (value: Record<string, unknown>) => {
          def.records.set(value[def.keyPath], value);
        },
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
    _version: number | undefined,
    opts: { upgrade?: (db: FakeUpgradeDb) => void },
  ) => {
    // Run the upgrade callback against a fresh upgrade view of the stores.
    // Reopen-at-current-version passes no `upgrade`, so it's optional.
    if (opts.upgrade) opts.upgrade(new FakeUpgradeDb(mockStores));
    return new FakeDb(mockStores) as unknown as import('idb').IDBPDatabase;
  },
);
const deleteDBMock = vi.fn(async () => undefined);

vi.mock('idb', () => ({
  openDB: (...args: unknown[]) =>
    (openDBMock as unknown as (...a: unknown[]) => unknown)(...args),
  deleteDB: (...args: unknown[]) =>
    (deleteDBMock as unknown as (...a: unknown[]) => unknown)(...args),
}));

// Import AFTER mocks are registered. Because initDb caches a module-level
// singleton promise, we re-import a fresh module copy in beforeEach via
// vi.resetModules() so each test starts clean.
let sessionDb: typeof import('./legacySessionDb');

beforeEach(async () => {
  mockStores = new Map();
  openDBMock.mockClear();
  openDBMock.mockReset();
  openDBMock.mockImplementation(async (_n: unknown, _v: unknown, opts: { upgrade?: (db: FakeUpgradeDb) => void }) => {
    if (opts.upgrade) opts.upgrade(new FakeUpgradeDb(mockStores));
    return new FakeDb(mockStores) as unknown as import('idb').IDBPDatabase;
  });
  deleteDBMock.mockClear();
  vi.resetModules();
  sessionDb = await import('./legacySessionDb');
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

describe('initDb — recovery paths', () => {
  it('reopens at the current version when the versioned open fails', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    openDBMock.mockRejectedValueOnce(new Error('VersionError')); // first (versioned) open
    const db = await sessionDb.initDb();
    expect(db).toBeDefined();
    expect(openDBMock).toHaveBeenCalledTimes(2);
    expect(openDBMock.mock.calls[1][1]).toBeUndefined(); // reopen passes version=undefined
    warn.mockRestore();
  });

  it('resets the DB when both the versioned open and the reopen fail', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    openDBMock.mockRejectedValueOnce(new Error('e1')); // versioned
    openDBMock.mockRejectedValueOnce(new Error('e2')); // reopen
    const db = await sessionDb.initDb(); // third call (recreate) succeeds
    expect(db).toBeDefined();
    expect(deleteDBMock).toHaveBeenCalledTimes(1);
    expect(openDBMock).toHaveBeenCalledTimes(3);
    warn.mockRestore();
  });

  it('still recovers when deleteDB itself fails during reset', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    openDBMock.mockRejectedValueOnce(new Error('e1')); // versioned
    openDBMock.mockRejectedValueOnce(new Error('e2')); // reopen
    deleteDBMock.mockRejectedValueOnce(new Error('del fail')); // .catch(() => {}) swallows it
    const db = await sessionDb.initDb(); // openVersioned() retry still succeeds
    expect(db).toBeDefined();
    warn.mockRestore();
  });

  it('runs the blocked / blocking / terminated lifecycle callbacks', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    // Throw from close() so BOTH the `.then(db => db.close())` and its
    // `.catch(() => {})` swallow-arrow on the same line are exercised.
    const closeSpy = vi.spyOn(FakeDb.prototype, 'close').mockImplementation(() => {
      throw new Error('close failed');
    });
    await sessionDb.initDb();
    const opts = openDBMock.mock.calls[0][2] as {
      blocked: () => void;
      blocking: () => void;
      terminated: () => void;
    };
    opts.blocked();
    expect(warn).toHaveBeenCalled();
    opts.blocking(); // closes the cached db + clears the singleton
    await vi.waitFor(() => expect(closeSpy).toHaveBeenCalled()); // db.close() ran
    await sessionDb.initDb(); // singleton was cleared → re-opens
    expect(openDBMock.mock.calls.length).toBeGreaterThanOrEqual(2);
    opts.terminated(); // also clears the singleton (no throw)
    closeSpy.mockRestore();
    warn.mockRestore();
  });
});

describe('listSessions — workspace scoping', () => {

  it('listSessions returns only the given workspace when groupId is provided', async () => {
    await sessionDb.initDb();
    const store = mockStores.get('sessions')!;
    store.records.set('x', { id: 'x', title: 'x', groupId: 'g1', createdAt: '2023-01-01', updatedAt: '2023-01-01' });
    store.records.set('y', { id: 'y', title: 'y', groupId: 'g2', createdAt: '2023-01-02', updatedAt: '2023-01-02' });
    const result = await sessionDb.listSessions('g1');
    expect(result.map((s) => s.id)).toEqual(['x']);
  });
});

describe('session running-job marker', () => {

  it('marker records are excluded from the session list', async () => {
    await sessionDb.initDb();
    const store = mockStores.get('sessions')!;
    store.records.set('s1', { id: 's1', title: 't', groupId: 'g1', createdAt: new Date(), updatedAt: new Date() });
    store.records.set('running-job:s1', { id: 'running-job:s1', runningJobId: 'job-1' });
    const sessions = await sessionDb.listSessions('g1');
    expect(sessions.map((s) => s.id)).toEqual(['s1']); // no phantom marker row
  });
});
