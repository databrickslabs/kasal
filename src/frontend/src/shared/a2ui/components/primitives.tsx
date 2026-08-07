/**
 * Leaf + layout renderers: text, media, containers.
 */
import { useContext, useMemo } from 'react'
import type { CSSProperties } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type { NodeProps } from '../types'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card'
import { Separator } from '../ui/separator'
import { DeckThemeContext, deckProseVars } from '../lib/deckThemes'
import { SurfaceContext } from '../lib/surfaceContext'
import { mdComponents, linkifyCitations } from '../lib/markdown'
import { cn } from '../lib/utils'
import { iconByName } from './icons'
import { SlideCtx } from './slideContext'
import { asArr, asStr } from './values'

export function Markdown({ node, resolve }: NodeProps) {
  // Inside a slide deck, drive the `prose` text colors from the deck theme so
  // body/bullets/headings contrast with the stage. Without this, prose keeps its
  // default near-black colors and disappears on a dark deck theme (the title and
  // kicker stay visible because they use explicit theme.* colors, the bullets
  // don't). Outside a deck (chat / document surfaces) prose keeps its defaults.
  const theme = useContext(DeckThemeContext)
  const { inDeck } = useContext(SlideCtx)
  const proseStyle = inDeck ? (deckProseVars(theme) as CSSProperties) : undefined
  return (
    <div
      className="prose prose-sm prose-neutral max-w-none dark:prose-invert prose-pre:bg-muted prose-pre:text-foreground prose-code:before:content-none prose-code:after:content-none"
      style={proseStyle}
    >
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={mdComponents}>
        {linkifyCitations(asStr(resolve(node.content)))}
      </ReactMarkdown>
    </div>
  )
}

export function Text({ node, resolve }: NodeProps) {
  const variant = asStr(node.variant) || 'body'
  return (
    <p
      className={cn(
        // my-0: spacing between text blocks comes from the container's gap/space
        // utilities, never the browser's default <p> margin. The export ships full
        // Tailwind preflight (so <p> is already margin-0); Kasal disables preflight
        // globally to protect MUI, so without this the default ~16px paragraph
        // margins STACK on the container gap and blow open big vertical voids (the
        // run-activity prose feed). Scoped to Text so it can't disturb anything else.
        'my-0 leading-relaxed',
        variant === 'caption' && 'text-sm text-muted-foreground',
        variant === 'label' && 'text-xs uppercase tracking-wide text-muted-foreground',
      )}
    >
      {asStr(resolve(node.text))}
    </p>
  )
}

export function Heading({ node, resolve }: NodeProps) {
  const level = Math.min(6, Math.max(1, Number(node.level) || 2))
  const Tag = (`h${level}` as unknown) as keyof JSX.IntrinsicElements
  const sizes: Record<number, string> = {
    1: 'text-2xl', 2: 'text-xl', 3: 'text-lg', 4: 'text-base', 5: 'text-sm', 6: 'text-sm',
  }
  return <Tag className={cn('my-1.5 font-semibold tracking-tight', sizes[level])}>{asStr(resolve(node.text))}</Tag>
}

export function Image({ node, resolve }: NodeProps) {
  const src = asStr(resolve(node.src))
  const caption = asStr(resolve(node.caption))
  return (
    <figure className="m-0">
      <img src={src} alt={asStr(node.alt) || caption} className="max-w-full rounded-lg" />
      {caption && <figcaption className="mt-1 text-sm text-muted-foreground">{caption}</figcaption>}
    </figure>
  )
}

export function Card_({ node, render }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const Icon = iconByName(node.icon)
  return (
    <Card className="bg-secondary/40">
      {node.title != null && (
        <CardHeader className="p-4 pb-2">
          <CardTitle className="flex items-center gap-2 text-base">
            {Icon && <Icon className="size-4 shrink-0" style={{ color: theme.accent }} aria-hidden="true" />}
            {asStr(node.title)}
          </CardTitle>
        </CardHeader>
      )}
      <CardContent className={cn('p-4', node.title != null && 'pt-0')}>
        {(node.children || []).map((id) => render(id))}
      </CardContent>
    </Card>
  )
}

export function KeyValue({ node, resolve }: NodeProps) {
  const { inDeck } = useContext(SlideCtx)
  const theme = useContext(DeckThemeContext)
  const Icon = iconByName(node.icon)
  if (inDeck) {
    // Big-number stat tile, themed to the active deck. `h-full` + flex column so a
    // row of tiles is always equal height (a longer label wrapping to two lines no
    // longer makes its tile taller than its neighbours).
    return (
      <div className="flex h-full flex-col rounded-xl border p-5" style={{ background: theme.panel, borderColor: theme.panelBorder }}>
        {Icon && <Icon className="mb-3 size-7 shrink-0" style={{ color: theme.accent }} aria-hidden="true" />}
        <div className="text-balance text-[2.2rem] font-extrabold leading-none" style={{ color: theme.accent }}>
          {asStr(resolve(node.value))}
        </div>
        {/* Label pinned to the BOTTOM of the tile (`mt-auto`), so labels sit on one
            line across the whole row. Stacked directly under the value they follow
            its height instead: one tile whose value wraps to two lines pushed its
            label down while its single-line neighbours kept theirs up, and the row
            read as misaligned.
            Color is the body foreground (not `muted`) so it stays legible — a
            workspace palette whose muted color sits near the surface color would
            otherwise wash the label out against the tile. */}
        <div className="mt-auto pt-2 text-sm font-medium" style={{ color: theme.fg, opacity: 0.85 }}>{asStr(resolve(node.label))}</div>
      </div>
    )
  }
  return (
    <div className="flex h-full flex-col rounded-xl border bg-secondary/40 p-4">
      {Icon && <Icon className="mb-2 size-5 shrink-0" style={{ color: theme.accent }} aria-hidden="true" />}
      <div className="text-2xl font-bold">{asStr(resolve(node.value))}</div>
      <div className="mt-1 text-sm font-medium text-foreground/80">{asStr(resolve(node.label))}</div>
    </div>
  )
}

export function List({ node, resolve }: NodeProps) {
  // Resolve the items binding AND each element — the model sometimes emits items
  // as an array of per-item bindings ([{path:"/options/0/title"}, ...]).
  const items = asArr(resolve(node.items)).map((it) => resolve(it))
  const Tag = node.ordered ? 'ol' : 'ul'
  return (
    <Tag className={cn('my-1.5 space-y-1 pl-6', node.ordered ? 'list-decimal' : 'list-disc')}>
      {items.map((it, i) => {
        // Items may arrive as objects ({title, description}); render them as
        // "title — description" instead of coercing to "[object Object]".
        if (it && typeof it === 'object') {
          const o = it as Record<string, any>
          const title = asStr(o.title ?? o.label ?? o.name ?? o.heading ?? o.text)
          const desc = asStr(o.description ?? o.detail ?? o.subtitle ?? o.body ?? '')
          if (!title && !desc) return <li key={i}>{asStr(it)}</li>
          return (
            <li key={i}>
              {title && <span className="font-medium">{title}</span>}
              {title && desc ? ' — ' : ''}
              {desc && <span className={title ? 'text-muted-foreground' : undefined}>{desc}</span>}
            </li>
          )
        }
        return <li key={i}>{asStr(it)}</li>
      })}
    </Tag>
  )
}

export function Divider() {
  return <Separator className="my-3" />
}

export function Row({ node, render }: NodeProps) {
  return (
    <div className="flex flex-wrap" style={{ gap: Number(node.gap) || 12 }}>
      {(node.children || []).map((id) => render(id))}
    </div>
  )
}

export function Column({ node, render }: NodeProps) {
  return (
    <div className="flex flex-col" style={{ gap: Number(node.gap) || 12 }}>
      {(node.children || []).map((id) => render(id))}
    </div>
  )
}

export function Grid({ node, render }: NodeProps) {
  const columns = Number(node.columns) || 2
  // A table reads terribly squeezed into one narrow grid cell (truncated columns,
  // tons of wrapping). Let any cell whose subtree contains a Table span the full
  // row instead — with grid auto-flow that drops it onto its own full-width row,
  // typically the bottom of the dashboard, which is where wide tables belong.
  const surface = useContext(SurfaceContext)
  const byId = useMemo(
    () => Object.fromEntries((surface?.components || []).map((c) => [c.id, c])),
    [surface],
  )
  const hasTable = (id: string, depth = 0): boolean => {
    if (depth > 6) return false
    const n = byId[id]
    if (!n) return false
    if (n.component === 'Table') return true
    return (Array.isArray(n.children) ? n.children : []).some((cid) => hasTable(cid, depth + 1))
  }
  // Lay out row-by-row instead of one fixed N-column grid. Normal cells pack
  // `columns` per row; an UNDERFULL last row uses its own item count as the
  // column count so its cells STRETCH to fill the width (symmetric, no empty
  // gap — e.g. 2 charts in a 3-col dashboard become two equal halves instead of
  // leaving a blank third cell). A Table-bearing cell always takes its OWN
  // full-width row (the wide footer).
  const children = node.children || []
  const rows: { wide: boolean; ids: string[] }[] = []
  let buf: string[] = []
  const flush = () => {
    if (buf.length) {
      rows.push({ wide: false, ids: buf })
      buf = []
    }
  }
  for (const id of children) {
    if (hasTable(id)) {
      flush()
      rows.push({ wide: true, ids: [id] })
    } else {
      buf.push(id)
      if (buf.length === columns) flush()
    }
  }
  flush()
  return (
    <div className="flex flex-col gap-3.5">
      {/* `h-full` on the cell wrapper gives stat tiles a defined stretched height
          so a row of tiles stays symmetric regardless of label length. */}
      {rows.map((row, ri) => (
        <div
          key={ri}
          className="grid items-stretch gap-3.5"
          style={{ gridTemplateColumns: row.wide ? '1fr' : `repeat(${row.ids.length}, minmax(0, 1fr))` }}
        >
          {row.ids.map((id) => (
            <div key={id} className="h-full">{render(id)}</div>
          ))}
        </div>
      ))}
    </div>
  )
}

export function Unsupported({ node }: NodeProps) {
  return <div className="text-sm italic text-muted-foreground">Unsupported component: {asStr(node.component)}</div>
}

// Exported as `Card` for the registry; the local name avoids clashing with the
// shadcn Card primitive imported above.
