/**
 * Presentation surface: slides, the deck shell and the download menu.
 *
 * Slides render their children through the `render` prop rather than importing
 * the component modules, which is what keeps this module free of cycles.
 */
import { Fragment, useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties } from 'react'
import type { ComponentNode, NodeProps } from '../types'
import { ChevronDown, Download, FileText, Presentation, StickyNote } from 'lucide-react'
import { Button } from '../ui/button'
import { downloadPptx } from '../lib/download'
import { DeckThemeContext, readableTextOn } from '../lib/deckThemes'
import { SurfaceContext, SurfaceChromeContext } from '../lib/surfaceContext'
import { cn } from '../lib/utils'
import { SlideCtx } from './slideContext'
import { FitBox } from './slideFit'
import { SurfaceDownloadMenu } from './surfaceDownload'
import { asNum, asStr } from './values'
import { normSlideSources } from '../lib/slideSources'

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

export function Slide({ node, render, resolve }: NodeProps) {
  const { idx, total } = useContext(SlideCtx)
  const theme = useContext(DeckThemeContext)
  const surface = useContext(SurfaceContext)
  // Normalize variant spellings ('two_column' / 'twocolumn' → 'two-column').
  const rawVariant = (asStr(node.variant) || 'content').toLowerCase().replace(/[_\s]/g, '-')
  const variant = rawVariant === 'twocolumn' ? 'two-column' : rawVariant
  const kicker = asStr(node.kicker)
  const subtitle = asStr(node.subtitle)
  const children = node.children || []
 const body = children.map((id) => render(id)).filter(Boolean)
  // Does the body actually render anything? (no/blank children → no.)
  const slideHasBody = useMemo(() => {
    const comps = surface?.components
    if (!comps) return children.length > 0
    const byId: Record<string, ComponentNode> = Object.fromEntries(comps.map((c) => [c.id, c]))
    return children.some((id) => nodeHasContent(id, byId))
  }, [surface, children])

  // Citations for the claims on this slide. Bound values are resolved (the
  // composer often points `sources` at a shared /sources list in the dataModel),
  // and the footer is laid out absolutely so adding attribution never reflows a
  // variant's body — a researched deck looks identical to an unsourced one apart
  // from the footer.
  const sources = useMemo(() => normSlideSources(resolve(node.sources)), [resolve, node.sources])
  const sourcesFooter = sources.length ? (
    <div
      className="a2-slide-sources absolute inset-x-14 bottom-4 flex flex-wrap items-baseline gap-x-4 gap-y-1 border-t pt-2 text-[0.78rem] leading-snug"
      style={{ borderColor: theme.panelBorder, color: theme.muted }}
    >
      <span className="font-semibold uppercase tracking-[0.14em]" style={{ color: theme.kicker }}>
        Sources
      </span>
      {sources.map((s, i) => (
        <span key={`${s.label}-${i}`} className="min-w-0">
          <span className="font-semibold">{i + 1}.</span>{' '}
          {s.url ? (
            <a href={s.url} target="_blank" rel="noopener noreferrer" className="underline underline-offset-2">
              {s.label}
            </a>
          ) : (
            s.label
          )}
        </span>
      ))}
    </div>
  ) : null

  // The sources footer is absolutely positioned, so every layout needs bottom
  // clearance when it is present or the body would run underneath it. Set inline
  // rather than as a `pb-*` class: Tailwind padding utilities have equal
  // specificity to each layout's own `p-*`/`py-*`, so which one wins would depend
  // on stylesheet order, not on the order written here.
  const stageStyle: CSSProperties = {
    background: theme.stage,
    color: theme.fg,
    ...(sources.length ? { paddingBottom: 76 } : {}),
  }

  // Slide chrome shared by every variant: the page number and (when present) the
  // sources footer. Emitted wherever a layout previously emitted `num` alone.
  const chrome = (
    <>
      <div className="absolute right-6 top-5 text-xs font-semibold tracking-wide" style={{ color: theme.muted }}>
        {idx + 1} / {total}
      </div>
      {sourcesFooter}
    </>
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
  const bodyVariant =
    variant === 'content' ||
    variant === 'two-column' ||
    variant === 'visual' ||
    variant === 'agenda' ||
    variant === 'comparison' ||
    variant === 'image-full' ||
    variant === 'kpi-split' ||
    variant === 'boxes' ||
    variant === 'split' ||
    variant === 'process' ||
    variant === 'icon-cards' ||
    variant === 'numbered-list' ||
    variant === 'contrast' ||
    variant === 'pillars' ||
    // 'big-number' belongs here too: its whole layout is the first child
    // rendered huge, so a childless one is an empty stage, not a headline.
    variant === 'big-number'
  // NOT 'hero' / 'end-card' / 'callout': those three are title-dominant BY
  // DESIGN (a keynote opener, an accent closing card, a pull-quote) and render
  // correctly with no children at all. Listing them here would redirect every
  // ordinary one to the centered section layout and throw away exactly what
  // distinguishes them — the 3.5rem title, the accent background, the blockquote.
  const titleOnlyContent = bodyVariant && !slideHasBody && node.title != null

  // The outline pre-pass knows every slide's title and layout minutes before its
  // body is written, and ships that skeleton so the deck's shape is on screen
  // while the slides are still being composed. Draw a pending slide as being
  // WRITTEN — its real title over placeholder bars — rather than as a finished
  // slide that happens to be empty, which is what it would otherwise look like.
  if (node.pending === true) {
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={stageStyle}>
        {chrome}
        {eyebrow}
        <div className="mb-5 mt-2 h-1.5 w-16 rounded-full" style={{ background: theme.accent }} />
        {node.title != null && (
          <h2 className="text-balance text-[2.5rem] font-bold leading-tight tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        <div className="mt-8 flex flex-1 flex-col gap-4" aria-busy="true" aria-label="Slide being written">
          {[0.92, 0.78, 0.85, 0.6].map((w, i) => (
            <div
              key={i}
              className="a2-slide-pending h-4 animate-pulse rounded"
              style={{ width: `${w * 100}%`, background: theme.panelBorder, animationDelay: `${i * 120}ms` }}
            />
          ))}
        </div>
      </div>
    )
  }
  if (variant === 'title' || variant === 'section' || titleOnlyContent) {
    return (
      <div
        className="a2-slide relative flex h-full flex-col items-center justify-center p-12 text-center"
        style={stageStyle}
      >
        {chrome}
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
    // Up to SIX per row: corporate templates routinely run a 5- or 6-tile
    // headline band (area / population / GDP / GDP-per-capita / urbanisation /
    // divisions), and a cap of 4 wrapped that into 4+2 — a ragged row where the
    // source is one line. `columns` lets a deck pin the count; otherwise it is
    // the child count, still bounded so 12 tiles cannot render unreadably.
    const cols = Math.min(Math.max(asNum(node.columns) || children.length, 1), 6)
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden p-10" style={stageStyle}>
        {chrome}
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
      <div className="a2-slide relative flex h-full flex-col justify-center p-12" style={stageStyle}>
        {chrome}
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

  if (variant === 'image-full') {
    // Full-bleed media with the title overlaid — the "chapter opener" slide.
    // Text is forced to white over a dark scrim rather than themed: the media is
    // an arbitrary photo, so a light theme's dark title would be unreadable on it.
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden" style={stageStyle}>
        <div className="absolute inset-0 [&_img]:size-full [&_img]:object-cover [&>*]:size-full">{body}</div>
        <div
          className="absolute inset-0"
          style={{ background: 'linear-gradient(0deg, rgba(0,0,0,0.82) 0%, rgba(0,0,0,0.35) 45%, rgba(0,0,0,0.1) 100%)' }}
        />
        <div className="relative mt-auto p-12 text-white">
          {kicker && (
            <div className="text-xs font-bold uppercase tracking-[0.18em] text-white/80">{kicker}</div>
          )}
          {node.title != null && (
            <h2 className="mt-3 max-w-4xl text-balance text-[2.7rem] font-extrabold leading-[1.05] tracking-tight">
              {asStr(node.title)}
            </h2>
          )}
          <div className="mt-5 h-1 w-20 rounded-full" style={{ background: theme.accent }} />
          {subtitle && <p className="mt-5 max-w-3xl text-pretty text-xl leading-relaxed text-white/90">{subtitle}</p>}
        </div>
        {chrome}
      </div>
    )
  }

  if (variant === 'hero') {
    // Keynote-style opening: massive centered title, subtitle, accent rule — no page number.
    // Use for the main deck opener after the title slide, or for a major section header.
    return (
      <div
        className="a2-slide relative flex h-full flex-col items-center justify-center p-12 text-center"
        style={stageStyle}
      >
        {chrome}
        {eyebrow}
        {node.title != null && (
          <h2 className="mt-3 text-balance text-[3.5rem] font-extrabold leading-[1.05] tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        <div className="mt-6 h-1 w-24 rounded-full" style={{ background: theme.accent }} />
        {subtitle && <p className="mt-6 max-w-2xl text-pretty text-2xl leading-relaxed" style={{ color: theme.muted }}>{subtitle}</p>}
        {children.length > 0 && <div className="mt-6 max-w-3xl space-y-2 text-left text-pretty">{body}</div>}
      </div>
    )
  }

  if (variant === 'big-number') {
    // One giant metric with label and context line — for the deck's headline number.
    // Children: first child is the number (KeyValue or Heading), rest is context.
    return (
      <div className="a2-slide relative flex h-full flex-col p-12" style={stageStyle}>
        {chrome}
        {eyebrow}
        {node.title != null && (
          <h2 className="text-balance text-2xl font-semibold tracking-tight" style={{ color: theme.muted }}>{asStr(node.title)}</h2>
        )}
        <div className="mt-8 flex-1 flex flex-col items-center justify-center">
          {body[0]}
          {body.length > 1 && (
            <div className="mt-6 max-w-2xl text-pretty text-lg text-center" style={{ color: theme.muted }}>
              {body.slice(1).map((c, i) => <div key={i}>{c}</div>)}
            </div>
          )}
        </div>
      </div>
    )
  }

  if (variant === 'end-card') {
    // Closing slide: accent background, centered takeaway — signals the deck is done.
    return (
      <div
        className="a2-slide relative flex h-full flex-col items-center justify-center p-12 text-center"
        style={{ ...stageStyle, background: theme.accent }}
      >
        {chrome}
        {eyebrow}
        {node.title != null && (
          <h2 className="mt-3 text-balance text-[3rem] font-extrabold leading-[1.05] tracking-tight text-white">
            {asStr(node.title)}
          </h2>
        )}
        {subtitle && <p className="mt-5 max-w-2xl text-pretty text-xl leading-relaxed text-white/90">{subtitle}</p>}
        {children.length > 0 && <div className="mt-6 max-w-3xl space-y-2 text-left text-pretty text-white/90">{body}</div>}
      </div>
    )
  }


  if (variant === 'process') {
    // Horizontal process flow: numbered steps in a row with connecting arrows.
    // Use for workflows, pipelines, stages, or step-by-step guides.
    // Each child is a Text node describing one step.
    // Keep the RENDERED node. `asStr(render(id))` coerces a React element to a
    // string, which is empty — every step card drew as a blank panel under a
    // numbered circle, on every process slide.
    const steps = children.map((id) => ({ id, node: render(id) }))
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={stageStyle}>
        {chrome}
        {eyebrow}
        {node.title != null && (
          <h2 className="text-balance text-[2.5rem] font-bold leading-tight tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        {subtitle && (
          <p className="mt-4 max-w-4xl text-pretty text-[1.4rem] leading-snug" style={{ color: theme.muted }}>{subtitle}</p>
        )}
        <div className="mt-8 flex min-w-0 flex-1 items-stretch gap-3">
          {steps.map((step, i) => (
            <Fragment key={step.id}>
              <div className="flex min-w-0 flex-1 flex-col items-center">
                <div
                  className="mb-3 flex size-12 shrink-0 items-center justify-center rounded-full text-xl font-extrabold"
                  style={{ background: theme.accent, color: '#fff' }}
                >
                  {i + 1}
                </div>
                <div
                  className="w-full min-w-0 flex-1 overflow-hidden rounded-xl border p-4 text-center text-[1.15rem] leading-snug"
                  style={{ background: theme.panel, borderColor: theme.panelBorder }}
                >
                  {step.node}
                </div>
              </div>
              {/* Between the columns, not below one: the arrow is a sibling of
                  the steps rather than a third child of a step's own column,
                  which is why they used to stack under the cards. Aligned to
                  the numbered circles so the row reads left-to-right. */}
              {i < steps.length - 1 && (
                <div
                  aria-hidden="true"
                  className="flex shrink-0 items-start pt-3 text-2xl leading-none"
                  style={{ color: theme.muted }}
                >
                  →
                </div>
              )}
            </Fragment>
          ))}
        </div>
      </div>
    )
  }

  if (variant === 'icon-cards') {
    // Feature/benefit cards in a grid, each with an icon placeholder and title/description.
    // Use for listing features, benefits, capabilities, or key points.
    // Each child should be a Card or KeyValue with title and description.
    const cols = Math.min(Math.max(children.length, 1), 3)
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={stageStyle}>
        {chrome}
        {eyebrow}
        {node.title != null && (
          <h2 className="text-balance text-[2.5rem] font-bold leading-tight tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        {subtitle && (
          <p className="mt-4 max-w-4xl text-pretty text-[1.4rem] leading-snug" style={{ color: theme.muted }}>{subtitle}</p>
        )}
        <div
          className="mt-7 grid flex-1 content-center gap-5"
          style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
        >
          {body}
        </div>
      </div>
    )
  }


  if (variant === 'numbered-list') {
    // Large numbered items for ranked / ordered points — each child gets a big
    // accent number badge and its own row. Use for top-3 lists, priorities, or
    // any ordered set where the sequence matters.
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={stageStyle}>
        {chrome}
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
        <div className="mt-7 flex min-h-0 flex-1 flex-col justify-center gap-6">
          {children.map((id, i) => (
            <div key={id} className="flex items-start gap-5">
              <span
                className="flex size-12 shrink-0 items-center justify-center rounded-full text-xl font-extrabold"
                style={{ background: theme.accent, color: 'white' }}
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

  if (variant === 'contrast') {
    // Before/after or problem/solution: two visually distinct panels separated
    // by a bold accent divider. Use when the answer contrasts two states,
    // approaches, or time periods. leftLabel / rightLabel name the sides.
    const labels = [asStr(node.leftLabel) || 'Before', asStr(node.rightLabel) || 'After']
    const mid = Math.ceil(children.length / 2)
    const columns = [children.slice(0, mid), children.slice(mid)]
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={stageStyle}>
        {chrome}
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
        <div className="mt-6 grid min-h-0 flex-1 grid-cols-[1fr_auto_1fr] items-stretch gap-0">
          {columns.map((col, side) => (
            <div
              key={side}
              className="flex min-w-0 flex-col px-6 py-5"
              style={{
                background: theme.panel,
                borderRight: side === 0 ? `2px solid ${theme.accent}` : 'none',
              }}
            >
              <div
                className="mb-4 text-base font-bold uppercase tracking-widest"
                style={{ color: side === 0 ? theme.accent : theme.kicker }}
              >
                {labels[side]}
              </div>
              <div className="flex min-w-0 flex-1 flex-col justify-center space-y-3 text-pretty text-[1.25rem] leading-relaxed [&_ul]:space-y-2 [&_ol]:space-y-2">
                {col.map((id) => render(id))}
              </div>
            </div>
          ))}
        </div>
      </div>
    )
  }

  if (variant === 'callout') {
    // A single dominant statement with decorative treatment — the slide's focus
    // is one punchy idea. Use for a key insight, a rule of thumb, or a principle
    // the audience should remember. Children are optional supporting detail.
    return (
      <div className="a2-slide relative flex h-full flex-col items-center justify-center p-12 text-center" style={stageStyle}>
        {chrome}
        {eyebrow}
        <div className="mx-auto mt-2 h-1 w-20 rounded-full" style={{ background: theme.accent }} />
        {node.title != null && (
          <blockquote className="mt-6 max-w-4xl text-balance text-[2.6rem] font-extrabold leading-[1.15]" style={{ color: theme.title }}>
            "{asStr(node.title)}"
          </blockquote>
        )}
        {subtitle && <p className="mt-5 max-w-2xl text-pretty text-xl leading-relaxed" style={{ color: theme.muted }}>{subtitle}</p>}
        {children.length > 0 && <div className="mt-6 max-w-3xl text-pretty text-[1.35rem] leading-relaxed" style={{ color: theme.muted }}>{body}</div>}
      </div>
    )
  }

  if (variant === 'pillars') {
    // 3-4 vertical pillars / columns for frameworks, models, or capability
    // categories. Each child becomes one pillar with a title and body.
    const cols = Math.min(Math.max(body.length, 1), 4)
    return (
      <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={stageStyle}>
        {chrome}
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
        <div
          className="mt-7 grid flex-1 content-center gap-5"
          style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
        >
          {body.map((child, i) => (
            <div
              key={i}
              className="flex min-h-0 flex-col rounded-2xl border px-5 py-6"
              style={{ borderColor: theme.panelBorder, background: theme.panel }}
            >
              {child}
            </div>
          ))}
        </div>
      </div>
    )
  }

  // Shared top-left header band (kicker → accent rule → title → subtitle) for the
  // two-column / visual / agenda layouts, mirroring the content layout's header.
  const header = (
    <>
      {chrome}
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
      <div className="a2-slide relative flex h-full flex-col overflow-hidden px-14 py-12" style={stageStyle}>
        {header}
        {/* One column when there is no visual — `grid-cols-2` reserved an empty
            media column and left the slide looking half-finished. */}
        <div
          className="mt-6 grid min-h-0 flex-1 items-stretch gap-10"
          style={{ gridTemplateColumns: right.length ? '1fr 1fr' : '1fr' }}
        >
          <FitBox className="flex flex-col justify-center space-y-4 text-pretty text-[1.35rem] leading-relaxed [&_ul]:space-y-3 [&_ol]:space-y-3">
            {left.map((id) => render(id))}
          </FitBox>
          {right.length > 0 && (
            <FitBox className="flex flex-col justify-center gap-4">
              {right.map((id) => render(id))}
            </FitBox>
          )}
        </div>
      </div>
    )
  }

  if (variant === 'visual') {
    // One dominant visual (Chart/Diagram/Table) with an optional caption — the
    // body fills the stage below the title instead of using content text sizes.
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden px-14 py-12" style={stageStyle}>
        {header}
        <FitBox
          outerClassName="relative mt-6 min-h-0 min-w-0 flex-1 overflow-hidden"
          className="flex flex-col justify-center gap-4 text-base"
        >
          {body}
        </FitBox>
      </div>
    )
  }

  if (variant === 'agenda') {
    // Numbered overview rows — each child (a short Text) gets an accent number
    // badge, the staple "agenda / what we'll cover" layout.
    //
    // `columns` flows the rows into more than one column. A 12-section contents
    // page in one column either overflows the stage or shrinks to unreadable;
    // two columns of six is how printed decks lay it out. Rows fill COLUMN-FIRST
    // (1-6 left, 7-12 right) so the numbering reads down each column, which is
    // what a reader scanning a contents page expects.
    const agendaCols = Math.min(Math.max(asNum(node.columns) || 1, 1), 3)
    const perCol = Math.ceil(children.length / agendaCols)
    const columns =
      agendaCols === 1
        ? [children]
        : Array.from({ length: agendaCols }, (_, c) => children.slice(c * perCol, (c + 1) * perCol))
    const byId: Record<string, ComponentNode> = Object.fromEntries((surface?.components || []).map((c) => [c.id, c]))
    const titleSize = agendaCols > 1 ? 'text-[1.15rem]' : 'text-[1.4rem]'
    const descSize = agendaCols > 1 ? 'text-[0.92rem]' : 'text-[1.1rem]'
    const badgeSize = agendaCols > 1 ? 'h-9 w-11 text-base' : 'h-11 w-14 text-xl'
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden px-14 py-12" style={stageStyle}>
        {header}
        <FitBox
          outerClassName="relative mt-6 min-h-0 min-w-0 flex-1 overflow-hidden"
          className="grid content-center gap-x-12"
          style={{ gridTemplateColumns: `repeat(${agendaCols}, minmax(0, 1fr))` }}
        >
          {columns.map((col, ci) => (
            <div key={ci} className={cn('flex flex-col justify-center', agendaCols > 1 ? 'gap-4' : 'gap-5')}>
              {col.map((id, i) => {
                const n = ci * perCol + i + 1
                // A contents row is a TITLE plus an optional descriptor, not one
                // run-on line. Written as "01 — Name — descriptor" (the natural way
                // to author it, and what the deck's task spec asks for), it wrapped
                // to three lines per row and repeated the number the badge already
                // shows. Split on the em-dashes so the row reads as a heading with a
                // caption under it, and drop a leading number that duplicates the
                // badge. Falls back to rendering the child untouched when there is
                // nothing to split — a plain agenda row still works.
                // ONLY a single-line Text/Heading row. A Markdown child is a
                // bullet list whose "\n- " separators this split would tear apart,
                // turning one panel into a title plus a mangled caption.
                const kind = byId[id]?.component
                const raw =
                  kind === 'Text' || kind === 'Heading' ? asStr(byId[id]?.text).trim() : ''
                const parts = raw && !raw.includes('\n') ? raw.split(/\s+[—–]\s+|\s+-\s+/) : []
                if (/^\d+$/.test((parts[0] || '').replace(/^0+/, '') || 'x')) parts.shift()
                const rowTitle = parts.shift() || ''
                const rowDesc = parts.join(' · ')
                return (
                  <div key={id} className="flex items-start gap-4">
                    {/* Square-ish accent tile with the zero-padded number, as the
                        template draws it — a filled tile anchors the row far better
                        than an outlined circle at slide distance. */}
                    <span
                      className={cn('flex shrink-0 items-center justify-center rounded font-extrabold tabular-nums', badgeSize)}
                      style={{ background: theme.accent, color: readableTextOn(theme.accent) }}
                    >
                      {String(n).padStart(2, '0')}
                    </span>
                    <div className="min-w-0 flex-1">
                      {rowTitle ? (
                        <>
                          <div className={cn('text-balance font-bold leading-tight', titleSize)} style={{ color: theme.title }}>
                            {rowTitle}
                          </div>
                          {rowDesc && (
                            <div className={cn('mt-1 text-pretty italic leading-snug', descSize)} style={{ color: theme.muted }}>
                              {rowDesc}
                            </div>
                          )}
                        </>
                      ) : (
                        <div className={cn('text-pretty leading-snug', titleSize)}>{render(id)}</div>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>
          ))}
        </FitBox>
      </div>
    )
  }

  if (variant === 'kpi-split') {
    // A headline KPI band over a split body — the workhorse of corporate country
    // and market briefs (14 of the 50 pages in the deck this was built for).
    // `stats` could not express it: that variant is tiles ONLY, so a slide with
    // tiles AND a body had to drop one or the other.
    //
    // KeyValue children form the band; everything else falls to the body, split
    // text-then-visual like 'two-column'. `ratio` sets the body columns, because
    // the same layout appears at 50/50, 60/40 (chart-led) and 40/60 (table-led)
    // and the difference is which side carries the argument.
    const byId: Record<string, ComponentNode> = Object.fromEntries((surface?.components || []).map((c) => [c.id, c]))
    const tileIds = children.filter((id) => byId[id]?.component === 'KeyValue')
    const rest = children.filter((id) => !tileIds.includes(id))
    const tileCols = Math.min(Math.max(tileIds.length, 1), 6)
    const visualIds = rest.filter((id) => byId[id] && SLIDE_VISUAL_COMPONENTS.has(byId[id].component))
    const textIds = rest.filter((id) => !visualIds.includes(id))
    // Text left, visual right when both exist; otherwise keep the emitted order
    // rather than inventing a split the content does not have.
    const mixed = visualIds.length > 0 && textIds.length > 0
    const mid = Math.ceil(rest.length / 2)
    const left = mixed ? textIds : rest.slice(0, mid)
    const right = mixed ? visualIds : rest.slice(mid)
    const ratio = asStr(node.ratio) || '50/50'
    // With nothing on the right, the body spans the FULL width. Keeping the two
    // columns reserved would squeeze the text into `ratio`'s share and leave the
    // rest of the slide blank — which is what happened whenever a spec asked for
    // "Markdown plus a Chart" and the facts did not support the chart, so the
    // agent (correctly) omitted it.
    const bodyCols = !right.length
      ? '1fr'
      : ratio === '60/40' ? '3fr 2fr' : ratio === '40/60' ? '2fr 3fr' : '1fr 1fr'
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden p-10" style={stageStyle}>
        {chrome}
        {eyebrow}
        {node.title != null && (
          <h2 className="mt-1 text-balance text-3xl font-bold tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        {tileIds.length > 0 && (
          <div
            className="mt-5 grid shrink-0 gap-4"
            style={{ gridTemplateColumns: `repeat(${tileCols}, minmax(0, 1fr))` }}
          >
            {tileIds.map((id) => render(id))}
          </div>
        )}
        {rest.length > 0 && (
          <div className="mt-6 grid min-h-0 flex-1 gap-8" style={{ gridTemplateColumns: bodyCols }}>
            {/* Fit PER COLUMN, not on the grid: the two columns overflow by
                different amounts (a long bullet list beside a chart that fits), and
                one shared factor would shrink the chart for the list's sake. */}
            <FitBox className="flex flex-col justify-center space-y-3 pr-1 text-pretty text-[1.15rem] leading-relaxed [&_ul]:space-y-2 [&_ol]:space-y-2">
              {left.map((id) => render(id))}
            </FitBox>
            {right.length > 0 && (
              <FitBox className="flex flex-col justify-center gap-3">{right.map((id) => render(id))}</FitBox>
            )}
          </div>
        )}
        {subtitle && <p className="mt-4 text-pretty text-sm" style={{ color: theme.muted }}>{subtitle}</p>}
      </div>
    )
  }

  if (variant === 'boxes') {
    // N titled panels on a fixed grid — the other recurring corporate shape (13
    // pages): challenge matrices, regulatory areas, solution maps, stakeholder
    // maps, scenario sets. `content` stacked these vertically and overflowed;
    // 'comparison' handles exactly two and no more.
    //
    // `columns` is honoured when given, else chosen so the cells stay legible:
    // 4 children read best 2x2, 6 as 3x2, 8 as 4x2.
    const n = children.length
    const cols = Math.min(
      Math.max(asNum(node.columns) || (n <= 2 ? n || 1 : n <= 4 ? 2 : n <= 6 ? 3 : 4), 1),
      4,
    )
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden p-10" style={stageStyle}>
        {chrome}
        {eyebrow}
        {node.title != null && (
          <h2 className="mt-1 text-balance text-3xl font-bold tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        {/* auto-rows-fr keeps every cell the same height, so a box with one line
            does not collapse next to a box with six — the template's panels are
            visually equal regardless of content length. */}
        <div
          className="mt-6 grid min-h-0 flex-1 auto-rows-fr gap-4 overflow-hidden"
          style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
        >
          {children.map((id) => (
            <div
              key={id}
              className="flex min-h-0 min-w-0 flex-col overflow-hidden rounded-lg border p-4"
              style={{ background: theme.panel, borderColor: theme.panelBorder }}
            >
              {/* Fit per CELL: `auto-rows-fr` gives every panel the same height, so
                  the panel with six bullets is the only one that needs shrinking and
                  the one-line panel keeps full size. Fitting the grid instead would
                  shrink all of them to the worst case. */}
              <FitBox className="flex flex-col text-pretty text-[1.05rem] leading-snug [&_ul]:space-y-1.5 [&_ol]:space-y-1.5">
                {render(id)}
              </FitBox>
            </div>
          ))}
        </div>
        {subtitle && <p className="mt-4 text-pretty text-sm" style={{ color: theme.muted }}>{subtitle}</p>}
      </div>
    )
  }

  if (variant === 'split') {
    // Two regions at an explicit ratio, with NO assumption about which side is
    // text and which is visual — that is what separates it from 'two-column'
    // (text-left/visual-right) and 'comparison' (two labelled peers). Needed for
    // the map-left/table-right and diagram-left/notes-right pages, where the
    // visual leads and the ratio is the whole point.
    const ratio = asStr(node.ratio) || '60/40'
    const mid = Math.ceil(children.length / 2)
    const left = children.slice(0, mid)
    const right = children.slice(mid)
    // Full width when there is no right-hand region — a spec that says "LEFT a
    // Sankey, RIGHT notes" yields ONE child whenever the facts do not support the
    // Sankey, and reserving its column leaves half the slide blank.
    const cols = !right.length
      ? '1fr'
      : ratio === '50/50' ? '1fr 1fr' : ratio === '40/60' ? '2fr 3fr' : ratio === '70/30' ? '7fr 3fr' : '3fr 2fr'
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden px-14 py-12" style={stageStyle}>
        {header}
        <div className="mt-6 grid min-h-0 flex-1 items-stretch gap-8" style={{ gridTemplateColumns: cols }}>
          <FitBox className="flex flex-col justify-center gap-4 pr-1 text-pretty text-[1.2rem] leading-relaxed [&_ul]:space-y-2 [&_ol]:space-y-2">
            {left.map((id) => render(id))}
          </FitBox>
          {right.length > 0 && (
            <FitBox className="flex flex-col justify-center gap-4 pr-1 text-pretty text-[1.2rem] leading-relaxed [&_ul]:space-y-2 [&_ol]:space-y-2">
              {right.map((id) => render(id))}
            </FitBox>
          )}
        </div>
      </div>
    )
  }

  if (variant === 'comparison') {
    // Two labelled panels side by side (A vs B). Distinct from 'two-column',
    // which is text-on-the-left / visual-on-the-right: here BOTH sides are peers
    // and each carries its own heading, so options, vendors, before/after states
    // or pros/cons read as a genuine comparison rather than a split body.
    const labels = [asStr(node.leftLabel), asStr(node.rightLabel)]
    const mid = Math.ceil(children.length / 2)
    const columns = [children.slice(0, mid), children.slice(mid)]
    return (
      <div className="a2-slide relative flex h-full flex-col overflow-hidden px-14 py-12" style={stageStyle}>
        {header}
        <div className="mt-6 grid min-h-0 flex-1 grid-cols-2 items-stretch gap-8">
          {columns.map((col, side) => (
            <div
              key={side}
              className="flex min-h-0 min-w-0 flex-col overflow-hidden rounded-2xl border p-6"
              style={{ background: theme.panel, borderColor: theme.panelBorder }}
            >
              {labels[side] && (
                <div
                  className="mb-4 border-b pb-3 text-lg font-bold tracking-tight"
                  style={{ color: side === 0 ? theme.accent : theme.kicker, borderColor: theme.panelBorder }}
                >
                  {labels[side]}
                </div>
              )}
              <FitBox className="flex flex-col justify-center space-y-3 text-pretty text-[1.25rem] leading-relaxed [&_ul]:space-y-2 [&_ol]:space-y-2">
                {col.map((id) => render(id))}
              </FitBox>
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
    <div className="a2-slide relative flex h-full flex-col px-14 py-12" style={stageStyle}>
      {chrome}
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
          avoid orphan words, and roomier inter-item rhythm. Wrapped in FitBox so a
          long bullet list SHRINKS to the stage instead of scrolling — a scrollbar
          is invisible in the downloaded PDF/PPTX, which silently loses the tail. */}
      <FitBox
        outerClassName="relative mt-6 min-h-0 min-w-0 flex-1 overflow-hidden"
        className="flex flex-col justify-center pr-1 text-pretty text-[1.55rem] leading-relaxed space-y-5 [&_ul]:space-y-3 [&_ol]:space-y-3 [&_li]:pl-1"
      >
        {body}
      </FitBox>
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


export function SlideDeck({ node, render, resolve }: NodeProps) {
  const slides = Array.isArray(node.children) ? node.children : []
  const total = slides.length
  const [idx, setIdx] = useState(0)
  const [showNotes, setShowNotes] = useState(false)
  const { fit } = useContext(SurfaceChromeContext)
  const surface = useContext(SurfaceContext)
  const theme = useContext(DeckThemeContext)
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
  // Speaker notes live off the 1280×720 canvas — they are the presenter's script,
  // not slide content — so they render under the deck and only when the current
  // slide actually has them. `deckNotes` gates the toggle so a deck with no notes
  // anywhere never shows a dead control.
  const byId = Object.fromEntries((surface?.components || []).map((c) => [c.id, c]))
  const noteFor = (id: string) => asStr(resolve(byId[id]?.notes)).trim()
  const deckHasNotes = slides.some((id) => noteFor(id) !== '')
  const currentNote = noteFor(slides[cur])
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
        <div className="flex items-center gap-2">
          {deckHasNotes && (
            <Button
              variant="outline"
              size="sm"
              aria-pressed={showNotes}
              onClick={() => setShowNotes((v) => !v)}
              title="Speaker notes"
            >
              <StickyNote className="mr-1.5 size-3.5" />
              Notes
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={() => setIdx((i) => clamp(i + 1))} disabled={cur === total - 1}>
            Next ›
          </Button>
        </div>
      </div>
      {showNotes && deckHasNotes && (
        <div
          className="shrink-0 overflow-auto rounded-xl border p-4 text-sm leading-relaxed"
          style={{ background: theme.panel, borderColor: theme.panelBorder, color: theme.fg, maxHeight: 200 }}
        >
          <div className="mb-1.5 text-xs font-bold uppercase tracking-[0.14em]" style={{ color: theme.kicker }}>
            Speaker notes · slide {cur + 1}
          </div>
          {currentNote ? (
            <p className="whitespace-pre-wrap text-pretty">{currentNote}</p>
          ) : (
            <p style={{ color: theme.muted }}>No notes for this slide.</p>
          )}
        </div>
      )}
    </div>
  )
}
