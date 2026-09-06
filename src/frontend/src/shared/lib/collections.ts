/** Index the first occurrence, preserving Array.find and stable deduplication semantics. */
export function firstBy<T, K>(items: readonly T[], key: (item: T) => K): Map<K, T> {
  const index = new Map<K, T>();
  for (const item of items) {
    const id = key(item);
    if (!index.has(id)) index.set(id, item);
  }
  return index;
}

export function uniqueBy<T, K>(items: readonly T[], key: (item: T) => K): T[] {
  return Array.from(firstBy(items, key).values());
}
