/**
 * Slide citation normalization.
 *
 * Lives in `lib/` rather than next to the Slide renderer because BOTH the
 * renderer and the PowerPoint exporter must read `sources` identically — a deck
 * whose citations differ on screen and in the downloaded .pptx is worse than one
 * with no citations at all. `lib/download` cannot import from `components/`
 * (the Slide renderer already imports `downloadPptx`, so that would be a cycle).
 */
import { asArr, asStr } from '../components/values'

export interface SlideSource {
  label: string
  url?: string
}

/**
 * Normalize a slide's `sources` prop to `{label, url}` entries.
 *
 * The composer copies whatever attribution the agent produced, so entries arrive
 * as plain strings, as objects under any of several key spellings, or as a mix.
 * A bare URL becomes its own label so a citation never renders blank, and
 * entries with nothing to show are dropped.
 */
export function normSlideSources(v: unknown): SlideSource[] {
  return asArr(v)
    .map((s): SlideSource | null => {
      if (typeof s === 'string') {
        const t = s.trim()
        return t ? { label: t, url: /^https?:\/\//i.test(t) ? t : undefined } : null
      }
      if (s && typeof s === 'object') {
        const o = s as Record<string, unknown>
        const url = asStr(o.url ?? o.href ?? o.link).trim() || undefined
        const label = asStr(o.label ?? o.title ?? o.name ?? o.source ?? o.publisher).trim() || url
        return label ? { label, url } : null
      }
      return null
    })
    .filter((s): s is SlideSource => s !== null)
}
