import type { Binding } from './types'

export function isBinding(v: unknown): v is Binding {
  return !!v && typeof v === 'object' && typeof (v as Binding).path === 'string'
}

// Resolve a literal-or-binding value against the dataModel using a JSON-pointer-ish
// path ("/a/0/b"). Returns the literal unchanged when it is not a binding.
export function resolveValue(value: unknown, dataModel: unknown): unknown {
  if (!isBinding(value)) return value
  const segments = value.path.replace(/^\//, '').split('/').filter(Boolean)
  return segments.reduce<unknown>((acc, key) => {
    if (acc == null) return acc
    const idx = Array.isArray(acc) ? Number(key) : key
    return (acc as Record<string, unknown>)[idx as never]
  }, dataModel)
}
