import type { Trace } from '../types/execution/trace';

const indexes = new WeakMap<Trace[], Set<string>>();
const compositeKey = (trace: Trace) =>
  `${trace.created_at}::${trace.event_type}::${trace.event_source}`;

/** Reuse the current array's membership index without retaining old snapshots. */
export function appendTraceBatch(existing: Trace[], incoming: Trace[]): Trace[] {
  let seen = indexes.get(existing);
  if (!seen) {
    seen = new Set<string>();
    for (const trace of existing) {
      seen.add(compositeKey(trace));
      if (trace.id != null) seen.add(`id:${trace.id}`);
    }
    indexes.set(existing, seen);
  }
  const fresh: Trace[] = [];
  for (const trace of incoming) {
    const key = compositeKey(trace);
    const id = trace.id != null ? `id:${trace.id}` : null;
    if (seen.has(key) || (id !== null && seen.has(id))) continue;
    // Transfer the index before mutating it: restoring or reusing an older
    // array must rebuild from that array, not inherit future membership.
    if (fresh.length === 0) indexes.delete(existing);
    seen.add(key);
    if (id !== null) seen.add(id);
    fresh.push(trace);
  }
  if (!fresh.length) return existing;
  const result = [...existing, ...fresh];
  indexes.set(result, seen);
  return result;
}
