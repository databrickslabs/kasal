import type { A2UIOverrides } from './A2UIRuntimeOverrides';

/** A workspace's stored A2UI overrides; anything unreadable counts as none. */
export function parseOverrides(raw: string | null | undefined): A2UIOverrides {
  if (!raw) return {};
  try {
    const data = JSON.parse(raw);
    if (!data || typeof data !== 'object' || Array.isArray(data)) return {};
    return Object.fromEntries(
      Object.entries(data).filter(([, v]) => typeof v === 'boolean' || typeof v === 'number'),
    ) as A2UIOverrides;
  } catch {
    return {};
  }
}
