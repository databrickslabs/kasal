type Payload = Record<string, unknown>;

interface Batch {
  payload: Payload;
  promise: Promise<void>;
  resolve: () => void;
  reject: (reason: unknown) => void;
}

interface Entry {
  pending?: Batch;
  running?: Promise<void>;
  timer?: ReturnType<typeof setTimeout>;
  ready: boolean;
}

/** One in-flight write and one merged pending snapshot per message. */
export function createMessageUpdateQueue(
  write: (id: string, payload: Payload) => Promise<void>,
  intervalMs = 250,
) {
  const entries = new Map<string, Entry>();

  const drain = (id: string, entry: Entry): void => {
    if (entry.running || !entry.pending || !entry.ready) return;
    const batch = entry.pending;
    entry.pending = undefined;
    entry.ready = false;
    clearTimeout(entry.timer);
    entry.timer = undefined;
    entry.running = batch.promise;
    // Keep the payload frozen once handed to the transport. New updates merge
    // into a separate pending batch, including while a create is still pending.
    void Promise.resolve().then(() => write(id, batch.payload)).then(
      batch.resolve,
      batch.reject,
    ).finally(() => {
      entry.running = undefined;
      if (entry.pending) drain(id, entry);
      else entries.delete(id);
    });
  };

  return {
    enqueue(id: string, payload: Payload, defer = false): Promise<void> {
      let entry = entries.get(id);
      if (!entry) {
        entry = { ready: false };
        entries.set(id, entry);
      }
      if (entry.pending) {
        // A content-only update must not erase a pending card/envelope update.
        // An explicitly newer envelope retains the API's replacement semantics.
        Object.assign(entry.pending.payload, payload);
      } else {
        let resolve!: () => void;
        let reject!: (reason: unknown) => void;
        const promise = new Promise<void>((ok, fail) => {
          resolve = ok;
          reject = fail;
        });
        entry.pending = { payload: { ...payload }, promise, resolve, reject };
        if (defer) {
          const scheduled = entry;
          entry.timer = setTimeout(() => {
            scheduled.timer = undefined;
            scheduled.ready = true;
            drain(id, scheduled);
          }, intervalMs);
        }
      }
      const result = entry.pending.promise;
      if (!defer) {
        clearTimeout(entry.timer);
        entry.timer = undefined;
        entry.ready = true;
      }
      drain(id, entry);
      return result;
    },

    /** Terminal transient updates also flush text that has not reached storage. */
    flush(id: string): Promise<void> {
      const entry = entries.get(id);
      if (!entry) return Promise.resolve();
      // A live entry always has a pending batch or an in-flight write.
      const result = entry.pending?.promise ?? entry.running!;
      clearTimeout(entry.timer);
      entry.timer = undefined;
      entry.ready = true;
      drain(id, entry);
      return result;
    },
  };
}
