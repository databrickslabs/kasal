/**
 * Value coercion helpers shared by every renderer.
 *
 * The model emits loosely-typed JSON, so a prop that can be data is coerced here
 * rather than trusted.
 */


// Pull a readable string from a value the model may have emitted as an object
// (e.g. {title, description}) instead of a plain string.
const pickText = (o: Record<string, any>): string | null => {
  const v = o.title ?? o.label ?? o.name ?? o.heading ?? o.text ?? o.value ?? o.content
  return v != null && typeof v !== 'object' ? String(v) : null
}
// Coerce any value to a string for display — never renders "[object Object]":
// objects fall back to a known text field, then to compact JSON.
export const asStr = (v: unknown): string => {
  if (v == null) return ''
  if (typeof v === 'object') {
    const t = pickText(v as Record<string, any>)
    if (t != null) return t
    try {
      return JSON.stringify(v)
    } catch {
      return ''
    }
  }
  return String(v)
}
export const asArr = (v: unknown): any[] => (Array.isArray(v) ? v : [])

// Parse a cell for numeric-aware sorting: strips the decorations models emit on
// figures (~ approx, thousands commas, currency, %, whitespace). Returns a number
// when the cleaned value is numeric, else null so the caller falls back to a
// locale string compare. Lets a "Total DBUs" column of "~905,930" sort as numbers.
export const numericValue = (s: string): number | null => {
  const cleaned = s.replace(/[~,$%\s]/g, '')
  if (!/^-?\d*\.?\d+$/.test(cleaned)) return null
  const n = Number(cleaned)
  return Number.isFinite(n) ? n : null
}

// Coerce a value to a finite number, or null. Tolerates numeric strings with
// thousands separators / units (e.g. "1,234", "12%") that models often emit.
export const asNum = (v: unknown): number | null => {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v.replace(/[,\s%$]/g, ''))
    return Number.isFinite(n) ? n : null
  }
  return null
}
