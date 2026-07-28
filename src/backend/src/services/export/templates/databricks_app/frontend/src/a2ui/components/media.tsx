/**
 * Gallery + map renderers. LeafletMap is lazy so leaflet stays out of the
 * main bundle.
 */
import { lazy, Suspense, useContext, useMemo, useState } from 'react'
import type { NodeProps } from '../types'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import { DeckThemeContext, seriesFromAccent } from '../lib/deckThemes'
import { asArr, asStr } from './values'

// ---- Album (image carousel) ----------------------------------------------
// A one-image-at-a-time carousel with prev/next. items is a list of image URLs
// or {src|url, caption?, href?}. The caption links to `href` (or the source URL)
// so a gallery built from search results stays clickable.
interface AlbumItem {
  src: string
  caption?: string
  href?: string
}
export function Album({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const items = useMemo<AlbumItem[]>(
    () =>
      asArr(resolve(node.items ?? node.images ?? node.photos))
        .map((it) => {
          if (it && typeof it === 'object') {
            const o = it as Record<string, unknown>
            const src = asStr(o.src ?? o.url ?? o.image ?? o.link)
            return { src, caption: asStr(o.caption ?? o.label ?? o.title) || undefined, href: asStr(o.href ?? o.link ?? o.url) || undefined }
          }
          return { src: asStr(it) }
        })
        .filter((it) => it.src),
    [resolve, node.items, node.images, node.photos],
  )
  const [idx, setIdx] = useState(0)
  if (!items.length) return null
  const at = Math.min(idx, items.length - 1)
  const cur = items[at]
  const go = (d: number) => setIdx((i) => (((i + d) % items.length) + items.length) % items.length)
  const navBtn =
    'absolute top-1/2 -translate-y-1/2 flex h-8 w-8 items-center justify-center rounded-full border bg-background/80 text-foreground shadow hover:bg-background'
  return (
    <div className="flex w-full min-w-0 flex-col">
      {node.title != null && <div className="mb-2 font-semibold">{asStr(node.title)}</div>}
      <div className="relative flex items-center justify-center overflow-hidden rounded-lg border" style={{ minHeight: 240, background: theme.panel }}>
        <img src={cur.src} alt={cur.caption || ''} className="max-h-[440px] max-w-full object-contain" />
        {items.length > 1 && (
          <>
            <button type="button" aria-label="Previous image" onClick={() => go(-1)} className={`${navBtn} left-2`}>
              <ChevronLeft className="size-5" />
            </button>
            <button type="button" aria-label="Next image" onClick={() => go(1)} className={`${navBtn} right-2`}>
              <ChevronRight className="size-5" />
            </button>
          </>
        )}
      </div>
      <div className="mt-2 flex items-center justify-between gap-3 text-sm">
        <span className="shrink-0 text-muted-foreground">{at + 1} / {items.length}</span>
        {cur.caption &&
          (cur.href ? (
            <a href={cur.href} target="_blank" rel="noreferrer" className="truncate underline">
              {cur.caption}
            </a>
          ) : (
            <span className="truncate">{cur.caption}</span>
          ))}
      </div>
    </div>
  )
}

// ---- GeoMap (real interactive map via react-leaflet + OpenStreetMap tiles) --
// Plots lat/lng points on an actual zoomable/pannable street map, auto-fit to the
// data. Registered under the name 'Map'. Leaflet is lazy-loaded (code-split) so it
// only downloads when a map surface actually renders. Needs network tiles — in an
// offline export / the PDF rasterizer the tiles won't load (markers still show).
type GeoPoint = {
  lat?: unknown
  lng?: unknown
  label?: unknown
  value?: unknown
}

const LeafletMap = lazy(() => import('../LeafletMap'))

export function GeoMap({ node, resolve }: NodeProps) {
  const title = asStr(resolve(node.title))
  const theme = useContext(DeckThemeContext)
  const pts = (asArr(resolve(node.points)) as GeoPoint[])
    .map((p) => ({ lat: Number(p.lat), lng: Number(p.lng), label: asStr(p.label), value: Number(p.value) }))
    .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lng))
  if (!pts.length) return null

  const hasValues = pts.some((p) => Number.isFinite(p.value) && p.value > 0)
  const maxVal = Math.max(...pts.map((p) => (Number.isFinite(p.value) ? p.value : 0)), 1)
  // Point/legend swatch colors follow the workspace accent (see Chart).
  const palette = seriesFromAccent(theme.accent, Math.max(pts.length, 1))
  const color = (i: number) => palette[i % palette.length]

  return (
    <div className="flex flex-col gap-3">
      {title && <h3 className="text-lg font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>}
      <div className="overflow-hidden rounded-2xl border" style={{ borderColor: theme.panelBorder }}>
        <Suspense
          fallback={
            <div className="flex items-center justify-center text-sm text-muted-foreground" style={{ height: 420 }}>
              Loading map…
            </div>
          }
        >
          <LeafletMap points={pts} hasValues={hasValues} maxVal={maxVal} />
        </Suspense>
      </div>
      {/* Legend (color swatch → label, optional value). Coloured with the chat's
          own page-text variable (always defined in #kasal-chat-root, flips with
          the chat theme) + a hardcoded fallback for the exported app — NOT the
          deck-stage `theme.fg` or the `--a2-foreground` token, which were unset /
          tuned for the dark map panel and washed out on the light page. */}
      <div className="grid gap-x-4 gap-y-1.5 text-xs sm:grid-cols-2" style={{ color: 'var(--text-primary, #1f2937)' }}>
        {pts.map((p, i) => (
          <div key={i} className="flex items-center gap-2 min-w-0">
            <span className="size-3 shrink-0 rounded-full" style={{ background: color(i) }} />
            <span className="truncate">{p.label || `${p.lat.toFixed(3)}°, ${p.lng.toFixed(3)}°`}</span>
            {hasValues && Number.isFinite(p.value) && p.value > 0 && (
              <span className="ml-auto shrink-0 font-semibold" style={{ color: 'var(--text-muted, #6b7280)' }}>{p.value}</span>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
