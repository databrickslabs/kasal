/**
 * Presentation surface: slides, the deck shell and the download menu.
 *
 * Slides render their children through the `render` prop rather than importing
 * the component modules, which is what keeps this module free of cycles.
 */
import { useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { ComponentNode, NodeProps } from '../types'
import { ChevronDown, Download, FileText, Presentation } from 'lucide-react'
import { Button } from '../ui/button'
import { downloadPptx } from '../lib/download'
import { DeckThemeContext } from '../lib/deckThemes'
import { SurfaceContext, SurfaceChromeContext } from '../lib/surfaceContext'
import { cn } from '../lib/utils'
import { SlideCtx } from './slideContext'
import { asStr } from './values'

// Whether a slide-child subtree carries any real content. A 'content' slide is
// "effectively empty" when it has no children OR only blank Text/Markdown — both
// render as a void below the title. Walks descendants so an empty bullet/markdown
// node doesn't slip through a naive children.length check.
function nodeHasContent(
  id: string,
  byId: Record<string, ComponentNode>,
  seen = new Set<string>(),
): boolean {
  if (seen.has(id)) return false
  seen.add(id)
  const n = byId[id]
  if (!n) return false
  if (n.component === 'Text' || n.component === 'Heading') return asStr(n.text).trim() !== ''
  if (n.component === 'Markdown') {
    const c = n.content
    return typeof c === 'string' ? c.trim() !== '' : c != null // binding → assume content
  }
  const kids = Array.isArray(n.children) ? n.children : []
  if (kids.length) return kids.some((k) => nodeHasContent(k, byId, seen))
  return true // a leaf visual component (KeyValue, image, chart, …) is content
}

// Components that read as a "visual" inside a slide — used by the two-column
// layout to decide which children sit in the media column.
const SLIDE_VISUAL_COMPONENTS = new Set([
  'Chart', 'Diagram', 'Table', 'Graph', 'Sequence', 'Forecast', 'Image', 'Album', 'Map',
])

export function Slide({ node, render }: NodeProps) {
  const { idx, total } = useContext(SlideCtx)
  const theme = useContext(DeckThemeContext)
  const surface = useContext(SurfaceContext)
  // Normalize variant spellings ('two_column' / 'twocolumn' → 'two-column').
  const rawVariant = (asStr(node.variant) || 'content').toLowerCase().replace(/[_\s]/g, '-')
  const variant = rawVariant === 'twocolumn' ? 'two-column' : rawVariant
  const kicker = asStr(node.kicker)
  const subtitle = asStr(node.subtitle)
  const children = node.children || []
  const body = children.map((id) => render(id))
  // Does the body actually render anything? (no/blank children → no.)
  const slideHasBody = useMemo(() => {
    const comps = surface?.components
    if (!comps) return children.length > 0
    const byId: Record<string, ComponentNode> = Object.fromEntries(comps.map((c) => [c.id, c]))
    return children.some((id) => nodeHasContent(id, byId))
  }, [surface, children])

  const num = (
    <div className="absolute right-6 top-5 text-xs font-semibold tracking-wide" style={{ color: theme.muted }}>
      {idx + 1} / {total}
    </div>
  )
  const eyebrow = kicker ? (
    <div className="text-xs font-bold uppercase tracking-[0.18em]" style={{ color: theme.kicker }}>
      {kicker}
    </div>
  ) : null

  // A body-bearing slide (content / two-column / visual / agenda) that ended up
  // with a title but NO body would render as a title stranded over a big empty
  // void — a broken-looking near-empty slide. Redirect it to the centered SECTION
  // layout so the lone title reads as a deliberate divider regardless of what the
  // generator emitted.
  const bodyVariant = variant === 'content' || variant === 'two-column' || variant === 'visual' || variant === 'agenda'
  const titleOnlyContent = bodyVariant && !slideHasBody && node.title != null
  if (variant === 'title' || variant === 'section' || titleOnlyContent) {
    return (
      <div
        className="a2-slide relative flex h-full flex-col items-center justify-center p-12 text-center"
        style={{ background: theme.stage, color: theme.fg }}
      >
        {num}
        {eyebrow}
        {node.title != null && (
          <h2 className="mt-3 text-balance text-[2.7rem] font-extrabold leading-[1.05] tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        <div className="mt-6 h-1 w-20 rounded-full" style={{ background: theme.accent }} />
        {subtitle && <p className="mt-6 max-w-2xl text-pretty text-xl leading-relaxed" style={{ color: theme.muted }}>{subtitle}</p>}
        {children.length > 0 && <div className="mt-6 max-w-3xl space-y-2 text-left text-pretty">{body}</div>}
      </div>
    )
  }

  if (variant === 'stats') {
    const cols = Math.min(Math.max(children.length, 1), 4)
    return (
      <div className="a2-slide relative flex h-full flex-col p-10" style={{ background: theme.stage, color: theme.fg }}>
        {num}
        {eyebrow}
        {node.title != null && (
          <h2 className="mt-1 text-balance text-3xl font-bold tracking-tight" style={{ color: theme.title }}>{asStr(node.title)}</h2>
        )}
        <div className="mt-7 grid flex-1 content-center gap-5" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
          {body}
        </div>
        {subtitle && <p className="mt-4 max-w-3xl text-pretty text-base" style={{ color: theme.muted }}>{subtitle}</p>}
      </div>
    )
  }

  if (variant === 'quote') {
    return (
      <div className="a2-slide relative flex h-full flex-col justify-center p-12" style={{ background: theme.stage, color: theme.fg }}>
        {num}
        {eyebrow}
        <div className="mb-5 mt-3 h-1 w-16 rounded-full" style={{ background: theme.accent }} />
        {node.title != null && (
          <blockquote className="max-w-4xl text-balance text-[2.2rem] font-semibold leading-snug" style={{ color: theme.title }}>
            “{asStr(node.title)}”
          </blockquote>
        )}
        {subtitle && <p className="mt-6 text-pretty text-lg font-medium" style={{ color: theme.kicker }}>— {subtitle}</p>}
        {children.length > 0 && <div className="mt-6 max-w-3xl text-pretty text-base" style={{ color: theme.muted }}>{body}</div>}
      </div>
    )
  }

  // Shared top-left header band (kicker → accent rule → title → subtitle) for the
  // two-column / visual / agenda layouts, mirroring the content layout's header.
  const header = (
    <>
      {num}
      {eyebrow}
      <div className="mb-5 mt-2 h-1.5 w-16 rounded-full" style={{ background: theme.accent }} />
      {node.title != null && (
        <h2 className="text-balance text-[2.5rem] font-bold leading-tight tracking-tight" style={{ color: theme.title }}>
          {asStr(node.title)}
        </h2>
      )}
      {subtitle && (
        <p className="mt-4 max-w-4xl text-pretty text-[1.4rem] leading-snug" style={{ color: theme.muted }}>{subtitle}</p>
      )}
    </>
  )

  if (variant === 'two-column') {
    // Text children in the left column, visual children (Chart/Diagram/Table/…)
    // in the right; when the model didn't mark a visual, fall back to an even split.
    const byId: Record<string, ComponentNode> = Object.fromEntries((surface?.components || []).map((c) => [c.id, c]))
    const visualIds = children.filter((id) => byId[id] && SLIDE_VISUAL_COMPONENTS.has(byId[id].component))
    const textIds = children.filter((id) => !visualIds.includes(id))
    const mid = Math.ceil(children.length / 2)
    const left = visualIds.length && textIds.length ? textIds : children.slice(0, mid)
    const right = visualIds.length && textIds.length ? visualIds : children.slice(mid)
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={{ background: theme.stage, color: theme.fg }}>
        {header}
        <div className="mt-6 grid min-h-0 flex-1 grid-cols-2 items-center gap-10">
          <div className="flex min-w-0 flex-col justify-center space-y-4 text-pretty text-[1.35rem] leading-relaxed [&_ul]:space-y-3 [&_ol]:space-y-3">
            {left.map((id) => render(id))}
          </div>
          <div className="flex min-w-0 flex-col justify-center gap-4">
            {right.map((id) => render(id))}
          </div>
        </div>
      </div>
    )
  }

  if (variant === 'visual') {
    // One dominant visual (Chart/Diagram/Table) with an optional caption — the
    // body fills the stage below the title instead of using content text sizes.
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={{ background: theme.stage, color: theme.fg }}>
        {header}
        <div className="mt-6 flex min-h-0 flex-1 flex-col justify-center gap-4 text-base">{body}</div>
      </div>
    )
  }

  if (variant === 'agenda') {
    // Numbered overview rows — each child (a short Text) gets an accent number
    // badge, the staple "agenda / what we'll cover" layout.
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={{ background: theme.stage, color: theme.fg }}>
        {header}
        <div className="mt-6 flex min-h-0 flex-1 flex-col justify-center gap-5">
          {children.map((id, i) => (
            <div key={id} className="flex items-center gap-5">
              <span
                className="flex size-10 shrink-0 items-center justify-center rounded-full text-lg font-extrabold"
                style={{ background: theme.panel, border: `1px solid ${theme.panelBorder}`, color: theme.accent }}
              >
                {i + 1}
              </span>
              <div className="min-w-0 flex-1 text-pretty text-[1.45rem] leading-snug">{render(id)}</div>
            </div>
          ))}
        </div>
      </div>
    )
  }

  // content (default). Sized for the 1280×720 design canvas (the whole slide is
  // then scaled to the stage), so text reads at slide proportions — not tiny. The
  // body is vertically CENTERED in the area below the title so a few bullets fill
  // the slide instead of clustering at the top over a void.
  return (
    <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={{ background: theme.stage, color: theme.fg }}>
      {num}
      {eyebrow}
      <div className="mb-5 mt-2 h-1.5 w-16 rounded-full" style={{ background: theme.accent }} />
      {node.title != null && (
        <h2 className="text-balance text-[2.5rem] font-bold leading-tight tracking-tight" style={{ color: theme.title }}>
          {asStr(node.title)}
        </h2>
      )}
      {/* Subtitle as a lead-in (was previously dropped on content slides) — a short
          framing sentence under the title gives the slide context before the body. */}
      {subtitle && (
        <p className="mt-4 max-w-4xl text-pretty text-[1.4rem] leading-snug" style={{ color: theme.muted }}>{subtitle}</p>
      )}
      {/* Body: vertically centred, uses the full slide width (a measure cap made
          short lines wrap early and leave the right half empty), pretty wrapping to
          avoid orphan words, and roomier inter-item rhythm. */}
      <div className="mt-6 flex-1 flex flex-col justify-center overflow-auto pr-1 text-pretty text-[1.55rem] leading-relaxed space-y-5 [&_ul]:space-y-3 [&_ol]:space-y-3 [&_li]:pl-1">{body}</div>
    </div>
  )
}

// A slide is authored on a FIXED 16:9 design canvas (1280×720) and scaled to fit
// whatever box the deck is shown in. Because the whole canvas scales as one unit,
// every size (text, padding, rules, child content) stays proportional and the
// slide shrinks/grows as a whole — instead of keeping fixed-rem text that
// overflows and clips once the stage gets smaller (e.g. when the preview pane's
// "Customize" panel opens above the deck and steals vertical space).
const SLIDE_W = 1280
const SLIDE_H = 720
function SlideStage({ children }: { children: React.ReactNode }) {
  const ref = useRef<HTMLDivElement>(null)
  const [scale, setScale] = useState(0)
  // Layout effect → measured & scaled before the browser paints, so there's no
  // unscaled flash live and the off-screen PDF/PPTX raster captures it correctly.
  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const measure = () => setScale(el.clientWidth / SLIDE_W)
    measure()
    if (typeof ResizeObserver === 'undefined') return
    const ro = new ResizeObserver(measure)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])
  return (
    <div ref={ref} className="absolute inset-0 overflow-hidden">
      <div
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          width: SLIDE_W,
          height: SLIDE_H,
          transformOrigin: 'top left',
          transform: `scale(${scale})`,
          // Hidden until the first measure so an unscaled (huge) frame never flashes.
          visibility: scale ? 'visible' : 'hidden',
        }}
      >
        {children}
      </div>
    </div>
  )
}

// ---- SurfaceDownloadMenu (shared download chrome) --------------------------
// One elegant "Download" dropdown reused by every surface. Self-detects what the
// surface can export: PDF (any surface, when the host wires `onDownloadPdf`) and
// PowerPoint (decks only, via the shared DOM-free pptxgenjs export). Renders
// nothing when downloads are suppressed or there's nothing to offer — so it's
// safe to drop into the renderer for ALL surfaces.
export function SurfaceDownloadMenu({ className }: { className?: string }) {
  const surface = useContext(SurfaceContext)
  const theme = useContext(DeckThemeContext)
  const { downloads: showDownloads, onDownloadPdf } = useContext(SurfaceChromeContext)
  const [open, setOpen] = useState(false)
  const [exporting, setExporting] = useState(false)
  const isDeck = !!surface?.components?.some((c) => c.id === surface.root && c.component === 'SlideDeck')
  // Export the WHOLE deck (from the surface in context) to PowerPoint. pptxgenjs
  // is loaded lazily inside downloadPptx, themed to match the on-screen deck.
  const onPptx = useCallback(async () => {
    if (!surface || exporting) return
    setExporting(true)
    try {
      await downloadPptx(surface, theme)
    } catch (err) {
      console.error('[a2ui] PPTX export failed', err)
    } finally {
      setExporting(false)
    }
  }, [surface, theme, exporting])

  if (!showDownloads) return null
  const options: { key: string; label: string; sub: string; icon: JSX.Element; onClick: () => void }[] = []
  if (onDownloadPdf) options.push({ key: 'pdf', label: 'PDF', sub: 'Portable document', icon: <FileText className="size-4" />, onClick: onDownloadPdf })
  if (isDeck) options.push({ key: 'pptx', label: 'PowerPoint', sub: 'Editable slides', icon: <Presentation className="size-4" />, onClick: () => void onPptx() })
  if (!options.length) return null

  return (
    <div className={cn('relative flex shrink-0 justify-start', className)}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        disabled={!surface || exporting}
        aria-haspopup="menu"
        aria-expanded={open}
        className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-2.5 py-1.5 text-xs font-medium text-muted-foreground shadow-sm transition-colors hover:bg-accent hover:text-accent-foreground disabled:cursor-not-allowed disabled:opacity-50"
      >
        <Download className="size-3.5" /> {exporting ? 'Preparing…' : 'Download'}
        <ChevronDown className={cn('size-3 transition-transform', open && 'rotate-180')} />
      </button>
      {open && (
        <>
          {/* click-away catcher */}
          <div className="fixed inset-0 z-10" onClick={() => setOpen(false)} aria-hidden="true" />
          <div
            role="menu"
            className="absolute left-0 top-9 z-20 min-w-[12rem] overflow-hidden rounded-xl border border-border bg-popover p-1 text-popover-foreground shadow-lg"
          >
            {options.map((opt) => (
              <button
                key={opt.key}
                type="button"
                role="menuitem"
                className="flex w-full items-center gap-2.5 rounded-lg px-2 py-1.5 text-left transition-colors hover:bg-accent hover:text-accent-foreground"
                onClick={() => {
                  setOpen(false)
                  opt.onClick()
                }}
              >
                <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-muted text-muted-foreground">{opt.icon}</span>
                <span className="flex min-w-0 flex-col">
                  <span className="text-xs font-semibold">{opt.label}</span>
                  <span className="text-[11px] text-muted-foreground">{opt.sub}</span>
                </span>
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

export function SlideDeck({ node, render }: NodeProps) {
  const slides = Array.isArray(node.children) ? node.children : []
  const total = slides.length
  const [idx, setIdx] = useState(0)
  const { fit } = useContext(SurfaceChromeContext)
  const clamp = (n: number) => Math.max(0, Math.min(total - 1, n))
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'ArrowRight') setIdx((i) => clamp(i + 1))
      if (e.key === 'ArrowLeft') setIdx((i) => clamp(i - 1))
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  })
  if (!total) return null
  const cur = clamp(idx)
  return (
    <div className={cn('flex flex-col gap-3', fit && 'h-full min-h-0')}>
      <SurfaceDownloadMenu />
      {/* 16:9 stage. aspectRatio is set INLINE (not via Tailwind's `aspect-video`
          utility) so the height is guaranteed regardless of JIT content scanning
          or preflight being disabled in the host app.
          - default (inline chat): width-driven (w-full), minHeight floor so it can
            never collapse and clip; the thread scrolls if it's tall.
          - fit (preview pane): height-driven + centered, so the whole slide fits the
            available height with NO vertical scroll (letterboxed left/right). */}
      {fit ? (
        <div className="flex min-h-0 flex-1 items-center justify-center">
          <div
            className="relative overflow-hidden rounded-2xl border shadow-sm"
            style={{ aspectRatio: '16 / 9', height: '100%', maxWidth: '100%' }}
          >
            <SlideStage>
              <SlideCtx.Provider value={{ idx: cur, total, inDeck: true }}>{render(slides[cur])}</SlideCtx.Provider>
            </SlideStage>
          </div>
        </div>
      ) : (
        <div
          className="relative w-full overflow-hidden rounded-2xl border shadow-sm"
          style={{ aspectRatio: '16 / 9', minHeight: 320 }}
        >
          <SlideStage>
            <SlideCtx.Provider value={{ idx: cur, total, inDeck: true }}>{render(slides[cur])}</SlideCtx.Provider>
          </SlideStage>
        </div>
      )}
      <div className="flex shrink-0 items-center justify-between gap-3">
        <Button variant="outline" size="sm" onClick={() => setIdx((i) => clamp(i - 1))} disabled={cur === 0}>
          ‹ Prev
        </Button>
        <div className="flex flex-wrap items-center justify-center gap-1.5">
          {slides.map((_, i) => (
            <button
              key={i}
              aria-label={`Go to slide ${i + 1}`}
              onClick={() => setIdx(i)}
              className={cn(
                'h-2 rounded-full transition-all',
                i === cur ? 'w-5 bg-primary' : 'w-2 bg-muted-foreground/30 hover:bg-muted-foreground/60',
              )}
            />
          ))}
        </div>
        <Button variant="outline" size="sm" onClick={() => setIdx((i) => clamp(i + 1))} disabled={cur === total - 1}>
          Next ›
        </Button>
      </div>
    </div>
  )
}
