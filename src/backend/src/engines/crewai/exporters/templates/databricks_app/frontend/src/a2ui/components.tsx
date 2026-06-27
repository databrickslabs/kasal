import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import {
  ResponsiveContainer,
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { NodeProps } from './types'
import { Download } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Separator } from '@/components/ui/separator'
import { Button } from '@/components/ui/button'
import { downloadCsv } from '@/lib/download'
import { DeckThemeContext } from '@/lib/deckThemes'
import { mdComponents, linkifyCitations } from '@/lib/markdown'
import { cn } from '@/lib/utils'

// Pull a readable string from a value the model may have emitted as an object
// (e.g. {title, description}) instead of a plain string.
const pickText = (o: Record<string, any>): string | null => {
  const v = o.title ?? o.label ?? o.name ?? o.heading ?? o.text ?? o.value ?? o.content
  return v != null && typeof v !== 'object' ? String(v) : null
}
// Coerce any value to a string for display — never renders "[object Object]":
// objects fall back to a known text field, then to compact JSON.
const asStr = (v: unknown): string => {
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
const asArr = (v: unknown): any[] => (Array.isArray(v) ? v : [])
const CHART_COLORS = ['#2563eb', '#10b981', '#f59e0b', '#ef4444', '#06b6d4', '#a855f7']

export function Markdown({ node, resolve }: NodeProps) {
  return (
    <div className="prose prose-sm prose-neutral max-w-none dark:prose-invert prose-pre:bg-muted prose-pre:text-foreground prose-code:before:content-none prose-code:after:content-none">
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
        'leading-relaxed',
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
  return (
    <Card className="bg-secondary/40">
      {node.title != null && (
        <CardHeader className="p-4 pb-2">
          <CardTitle className="text-base">{asStr(node.title)}</CardTitle>
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
  if (inDeck) {
    // Big-number stat tile, themed to the active deck.
    return (
      <div className="rounded-xl border p-5" style={{ background: theme.panel, borderColor: theme.panelBorder }}>
        <div className="text-[2.2rem] font-extrabold leading-none" style={{ color: theme.accent }}>
          {asStr(resolve(node.value))}
        </div>
        <div className="mt-2 text-sm" style={{ color: theme.muted }}>{asStr(resolve(node.label))}</div>
      </div>
    )
  }
  return (
    <div className="rounded-xl border bg-secondary/40 p-4">
      <div className="text-2xl font-bold">{asStr(resolve(node.value))}</div>
      <div className="mt-1 text-sm text-muted-foreground">{asStr(resolve(node.label))}</div>
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

export function Table({ node, resolve }: NodeProps) {
  const columns = asArr(node.columns)
  const rows = asArr(resolve(node.rows))
  return (
    <div>
      <div className="mb-1 flex justify-end">
        <Button
          variant="ghost"
          size="sm"
          className="h-7 gap-1 px-2 text-xs text-muted-foreground"
          onClick={() => downloadCsv(columns.map(asStr), rows.map((r) => asArr(r).map(asStr)), 'table.csv')}
        >
          <Download className="size-3.5" /> CSV
        </Button>
      </div>
      <div className="overflow-x-auto">
      <table className="w-full border-collapse text-sm">
        {columns.length > 0 && (
          <thead>
            <tr>
              {columns.map((c, i) => (
                <th key={i} className="border bg-muted px-3 py-2 text-left font-semibold">{asStr(c)}</th>
              ))}
            </tr>
          </thead>
        )}
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri}>
              {asArr(row).map((cell, ci) => (
                <td key={ci} className="border px-3 py-2">{asStr(cell)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      </div>
    </div>
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
  return (
    <div className="grid gap-3.5" style={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}>
      {(node.children || []).map((id) => render(id))}
    </div>
  )
}

export function Chart({ node, resolve }: NodeProps) {
  const type = asStr(node.chartType) || 'bar'
  const data = asArr(resolve(node.data))
  const xKey = asStr(node.xKey) || 'name'
  const yKeys = asArr(node.yKeys).map(asStr)
  const keys = yKeys.length ? yKeys : ['value']
  return (
    <div>
      {node.title != null && <div className="mb-2 font-semibold">{asStr(node.title)}</div>}
      <ResponsiveContainer width="100%" height={260}>
        {type === 'pie' ? (
          <PieChart>
            <Pie data={data} dataKey={keys[0]} nameKey={xKey} outerRadius={90} label>
              {data.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
            </Pie>
            <Tooltip /><Legend />
          </PieChart>
        ) : type === 'line' ? (
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
            {keys.map((k, i) => <Line key={k} type="monotone" dataKey={k} stroke={CHART_COLORS[i % CHART_COLORS.length]} />)}
          </LineChart>
        ) : (
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
            {keys.map((k, i) => <Bar key={k} dataKey={k} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
          </BarChart>
        )}
      </ResponsiveContainer>
    </div>
  )
}

// Slide layout context: which slide is showing + that we're inside a deck (so
// KeyValue renders as a themed stat tile). The deck THEME comes from
// DeckThemeContext (one theme for the whole deck — variety is by LAYOUT below).
const SlideCtx = createContext<{ idx: number; total: number; inDeck: boolean }>({
  idx: 0,
  total: 1,
  inDeck: false,
})

export function Slide({ node, render }: NodeProps) {
  const { idx, total } = useContext(SlideCtx)
  const theme = useContext(DeckThemeContext)
  const variant = (asStr(node.variant) || 'content').toLowerCase()
  const kicker = asStr(node.kicker)
  const subtitle = asStr(node.subtitle)
  const children = node.children || []
  const body = children.map((id) => render(id))

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

  if (variant === 'title' || variant === 'section') {
    return (
      <div
        className="a2-slide relative flex h-full flex-col items-center justify-center p-12 text-center"
        style={{ background: theme.stage, color: theme.fg }}
      >
        {num}
        {eyebrow}
        {node.title != null && (
          <h2 className="mt-3 text-[2.6rem] font-extrabold leading-[1.05] tracking-tight" style={{ color: theme.title }}>
            {asStr(node.title)}
          </h2>
        )}
        <div className="mt-5 h-1 w-20 rounded-full" style={{ background: theme.accent }} />
        {subtitle && <p className="mt-5 max-w-2xl text-lg" style={{ color: theme.muted }}>{subtitle}</p>}
        {children.length > 0 && <div className="mt-6 max-w-3xl space-y-2 text-left">{body}</div>}
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
          <h2 className="mt-1 text-3xl font-bold tracking-tight" style={{ color: theme.title }}>{asStr(node.title)}</h2>
        )}
        <div className="mt-7 grid flex-1 content-center gap-4" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
          {body}
        </div>
        {subtitle && <p className="mt-4 text-sm" style={{ color: theme.muted }}>{subtitle}</p>}
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
          <blockquote className="text-[2rem] font-semibold leading-snug" style={{ color: theme.title }}>
            “{asStr(node.title)}”
          </blockquote>
        )}
        {children.length > 0 && <div className="mt-6 text-base" style={{ color: theme.muted }}>{body}</div>}
      </div>
    )
  }

  // content (default)
  return (
    <div className="a2-slide relative flex h-full flex-col p-10" style={{ background: theme.stage, color: theme.fg }}>
      {num}
      {eyebrow}
      <div className="mb-4 mt-2 h-1.5 w-14 rounded-full" style={{ background: theme.accent }} />
      {node.title != null && (
        <h2 className="text-[1.9rem] font-bold leading-tight tracking-tight" style={{ color: theme.title }}>
          {asStr(node.title)}
        </h2>
      )}
      <div className="mt-5 flex-1 space-y-3 overflow-auto pr-1 text-[15px] leading-relaxed">{body}</div>
    </div>
  )
}

export function SlideDeck({ node, render }: NodeProps) {
  const slides = node.children || []
  const total = slides.length
  const [idx, setIdx] = useState(0)
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
    <div className="flex flex-col gap-3">
      <div className="relative aspect-video w-full overflow-hidden rounded-2xl border shadow-sm">
        <SlideCtx.Provider value={{ idx: cur, total, inDeck: true }}>{render(slides[cur])}</SlideCtx.Provider>
      </div>
      <div className="flex items-center justify-between gap-3">
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

// ---- Mindmap (interactive canvas, mirrors Kasal's ChatMode renderer) -------
// A tidy BILATERAL tree (root centered, branches split left/right) drawn with
// curved SVG connectors; pan the canvas, drag a node (carries its subtree),
// wheel-zoom, and collapse/expand branches. Light-themed for this app.
interface MindmapData {
  label?: unknown
  text?: unknown
  description?: unknown
  detail?: unknown
  note?: unknown
  children?: MindmapData[]
}
type XY = { x: number; y: number }
interface MMNode {
  id: string
  label: string
  detail: string
  depth: number
  parentId: string | null
  childIds: string[]
  color: string
}

const MM_NODE_W = 220
const MM_COL = 260
const MM_ROW = 76
const MM_MIN_ZOOM = 0.3
const MM_MAX_ZOOM = 2.5
const MM_ACCENT = '#2563eb'
const MM_TEXT = '#1f2330'
const MM_MUTED = '#6b7280'
const MM_BORDER = '#e6e8eb'
const MM_NODE_BG = '#ffffff'

function mindmapChildren(node: MindmapData): MindmapData[] {
  return Array.isArray(node.children)
    ? node.children.filter((c): c is MindmapData => Boolean(c) && typeof c === 'object')
    : []
}

function buildMindmap(root: MindmapData): { nodes: Record<string, MMNode>; rootId: string } {
  const nodes: Record<string, MMNode> = {}
  const walk = (node: MindmapData, id: string, depth: number, parentId: string | null, color: string) => {
    const kids = mindmapChildren(node)
    const childIds = kids.map((_, i) => `${id}.${i}`)
    const label = String(node.label ?? node.text ?? '')
    const explicit = node.description ?? node.detail ?? node.note
    const textVal = node.text != null ? String(node.text) : ''
    const detail = explicit != null ? String(explicit) : textVal && textVal !== label ? textVal : ''
    nodes[id] = { id, label, detail, depth, parentId, childIds, color }
    kids.forEach((k, i) => {
      const childColor = depth === 0 ? CHART_COLORS[i % CHART_COLORS.length] : color
      walk(k, childIds[i], depth + 1, id, childColor)
    })
  }
  walk(root, 'r', 0, null, MM_ACCENT)
  return { nodes, rootId: 'r' }
}

function leafCount(nodes: Record<string, MMNode>, id: string): number {
  const n = nodes[id]
  return n.childIds.length === 0 ? 1 : n.childIds.reduce((s, c) => s + leafCount(nodes, c), 0)
}

function descendantsOf(nodes: Record<string, MMNode>, id: string): string[] {
  const out: string[] = []
  const stack = [...nodes[id].childIds]
  while (stack.length) {
    const cur = stack.pop() as string
    out.push(cur)
    stack.push(...nodes[cur].childIds)
  }
  return out
}

function layoutMindmap(nodes: Record<string, MMNode>, rootId: string): Record<string, XY> {
  const pos: Record<string, XY> = {}
  const root = nodes[rootId]
  const right: string[] = []
  const left: string[] = []
  let rightLeaves = 0
  let leftLeaves = 0
  for (const branchId of root.childIds) {
    const leaves = leafCount(nodes, branchId)
    if (leftLeaves < rightLeaves) {
      left.push(branchId)
      leftLeaves += leaves
    } else {
      right.push(branchId)
      rightLeaves += leaves
    }
  }
  const placeSide = (branchIds: string[], sign: 1 | -1) => {
    let nextLeaf = 0
    const place = (id: string): number => {
      const node = nodes[id]
      const x = sign * node.depth * MM_COL
      let y: number
      if (node.childIds.length === 0) {
        y = nextLeaf * MM_ROW
        nextLeaf += 1
      } else {
        const ys = node.childIds.map(place)
        y = (ys[0] + ys[ys.length - 1]) / 2
      }
      pos[id] = { x, y }
      return y
    }
    branchIds.forEach(place)
  }
  placeSide(right, 1)
  placeSide(left, -1)
  const rightHeight = Math.max(0, rightLeaves - 1) * MM_ROW
  const leftHeight = Math.max(0, leftLeaves - 1) * MM_ROW
  const mid = Math.max(rightHeight, leftHeight) / 2
  const shiftSide = (branchIds: string[], height: number) => {
    const offset = mid - height / 2
    if (offset === 0) return
    for (const branchId of branchIds) {
      for (const id of [branchId, ...descendantsOf(nodes, branchId)]) {
        pos[id] = { ...pos[id], y: pos[id].y + offset }
      }
    }
  }
  shiftSide(right, rightHeight)
  shiftSide(left, leftHeight)
  pos[rootId] = { x: 0, y: mid }
  const minX = Math.min(...Object.values(pos).map((p) => p.x))
  if (minX !== 0) for (const id of Object.keys(pos)) pos[id] = { ...pos[id], x: pos[id].x - minX }
  return pos
}

function MindmapCanvas({ root }: { root: MindmapData }) {
  const { nodes, rootId } = useMemo(() => buildMindmap(root), [root])
  const initial = useMemo(() => layoutMindmap(nodes, rootId), [nodes, rootId])
  const [positions, setPositions] = useState<Record<string, XY>>(() => initial)
  const [collapsed, setCollapsed] = useState<Set<string>>(
    () => new Set(Object.values(nodes).filter((n) => n.depth >= 2 && n.childIds.length > 0).map((n) => n.id)),
  )
  const [view, setView] = useState({ scale: 1, x: 48, y: 32 })
  const [grabbing, setGrabbing] = useState(false)
  const [hovered, setHovered] = useState<string | null>(null)

  const positionsRef = useRef(positions)
  positionsRef.current = positions
  const sizeRef = useRef({ w: 0, h: 0 })
  const wheelCleanup = useRef<(() => void) | null>(null)
  const dragRef = useRef<
    | { mode: 'pan'; startX: number; startY: number; panStart: XY }
    | { mode: 'node'; startX: number; startY: number; ids: string[]; orig: Record<string, XY> }
    | null
  >(null)

  const toggle = (id: string) =>
    setCollapsed((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  const visible = useMemo(() => {
    const vis = new Set<string>()
    const stack = [rootId]
    while (stack.length) {
      const id = stack.pop() as string
      vis.add(id)
      if (!collapsed.has(id)) nodes[id].childIds.forEach((c) => stack.push(c))
    }
    return vis
  }, [nodes, rootId, collapsed])

  const zoomAt = useCallback((factor: number, cx: number, cy: number) => {
    setView((v) => {
      const scale = Math.min(MM_MAX_ZOOM, Math.max(MM_MIN_ZOOM, v.scale * factor))
      const k = scale / v.scale
      return { scale, x: cx - (cx - v.x) * k, y: cy - (cy - v.y) * k }
    })
  }, [])

  const centerView = useCallback(() => {
    const { w, h } = sizeRef.current
    const p = positionsRef.current[rootId]
    setView({ scale: 1, x: w / 2 - p.x, y: h / 2 - p.y })
  }, [rootId])

  const canvasRefCb = useCallback(
    (el: HTMLDivElement | null) => {
      if (wheelCleanup.current) {
        wheelCleanup.current()
        wheelCleanup.current = null
      }
      if (el) {
        sizeRef.current = { w: el.clientWidth, h: el.clientHeight }
        const handler = (e: WheelEvent) => {
          e.preventDefault()
          const r = el.getBoundingClientRect()
          zoomAt(e.deltaY < 0 ? 1.1 : 1 / 1.1, e.clientX - r.left, e.clientY - r.top)
        }
        el.addEventListener('wheel', handler, { passive: false })
        wheelCleanup.current = () => el.removeEventListener('wheel', handler)
        centerView()
      }
    },
    [zoomAt, centerView],
  )

  const zoomButton = (factor: number) => () => zoomAt(factor, sizeRef.current.w / 2, sizeRef.current.h / 2)

  const startNodeDrag = (id: string) => (e: React.PointerEvent) => {
    e.stopPropagation()
    const ids = [id, ...descendantsOf(nodes, id)]
    const orig: Record<string, XY> = {}
    ids.forEach((d) => (orig[d] = positions[d]))
    dragRef.current = { mode: 'node', startX: e.clientX, startY: e.clientY, ids, orig }
    setGrabbing(true)
  }
  const startPan = (e: React.PointerEvent) => {
    dragRef.current = { mode: 'pan', startX: e.clientX, startY: e.clientY, panStart: { x: view.x, y: view.y } }
    setGrabbing(true)
  }
  const onMove = (e: React.PointerEvent) => {
    const d = dragRef.current
    if (!d) return
    const dx = e.clientX - d.startX
    const dy = e.clientY - d.startY
    if (d.mode === 'pan') {
      setView((v) => ({ ...v, x: d.panStart.x + dx, y: d.panStart.y + dy }))
    } else {
      setPositions((prev) => {
        const next = { ...prev }
        d.ids.forEach((id) => (next[id] = { x: d.orig[id].x + dx / view.scale, y: d.orig[id].y + dy / view.scale }))
        return next
      })
    }
  }
  const endDrag = () => {
    dragRef.current = null
    setGrabbing(false)
  }

  const visibleIds = Object.keys(positions).filter((id) => visible.has(id))
  const maxX = visibleIds.reduce((m, id) => Math.max(m, positions[id].x), 0) + 240
  const maxY = visibleIds.reduce((m, id) => Math.max(m, positions[id].y), 0) + 140
  const gridSize = 22 * view.scale

  return (
    <div
      ref={canvasRefCb}
      onPointerDown={startPan}
      onPointerMove={onMove}
      onPointerUp={endDrag}
      onPointerLeave={endDrag}
      style={{
        position: 'relative',
        height: '64vh',
        minHeight: 460,
        overflow: 'hidden',
        borderRadius: 14,
        border: `1px solid ${MM_BORDER}`,
        background: '#fbfcfd',
        cursor: grabbing ? 'grabbing' : 'grab',
        touchAction: 'none',
        backgroundImage: `radial-gradient(${MM_BORDER} 1px, transparent 1px)`,
        backgroundSize: `${gridSize}px ${gridSize}px`,
        backgroundPosition: `${view.x}px ${view.y}px`,
      }}
    >
      <div
        style={{
          position: 'absolute',
          left: 0,
          top: 0,
          transformOrigin: '0 0',
          transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})`,
        }}
      >
        <svg
          width={maxX}
          height={maxY}
          style={{ position: 'absolute', left: 0, top: 0, overflow: 'visible', pointerEvents: 'none', zIndex: 0 }}
        >
          {visibleIds
            .filter((id) => nodes[id].parentId !== null)
            .map((id) => {
              const a = positions[nodes[id].parentId as string]
              const b = positions[id]
              const midX = (a.x + b.x) / 2
              return (
                <path
                  key={id}
                  d={`M ${a.x} ${a.y} C ${midX} ${a.y} ${midX} ${b.y} ${b.x} ${b.y}`}
                  fill="none"
                  stroke={nodes[id].color}
                  strokeWidth={2}
                  strokeOpacity={0.85}
                />
              )
            })}
        </svg>
        {visibleIds.map((id) => {
          const node = nodes[id]
          const isRoot = node.parentId === null
          const p = positions[id]
          const hasKids = node.childIds.length > 0
          const isCollapsed = collapsed.has(id)
          const onLeft = !isRoot && p.x < positions[rootId].x
          return (
            <div
              key={id}
              onPointerDown={startNodeDrag(id)}
              onMouseEnter={() => setHovered(id)}
              onMouseLeave={() => setHovered((h) => (h === id ? null : h))}
              style={{
                position: 'absolute',
                left: p.x,
                top: p.y,
                zIndex: 1,
                transform: 'translate(-50%, -50%)',
                display: 'inline-flex',
                flexDirection: onLeft ? 'row-reverse' : 'row',
                alignItems: 'center',
                gap: 8,
                cursor: 'grab',
                userSelect: 'none',
                touchAction: 'none',
                background: isRoot ? MM_ACCENT : MM_NODE_BG,
                color: isRoot ? '#ffffff' : MM_TEXT,
                border: `1px solid ${isRoot ? MM_ACCENT : MM_BORDER}`,
                ...(isRoot
                  ? {}
                  : onLeft
                    ? { borderRight: `3px solid ${node.color}` }
                    : { borderLeft: `3px solid ${node.color}` }),
                borderRadius: isRoot ? 14 : 11,
                padding: isRoot ? '11px 17px' : '8px 13px',
                fontWeight: isRoot ? 800 : 600,
                fontSize: isRoot ? '1.02rem' : '0.9rem',
                width: isRoot ? MM_NODE_W + 20 : MM_NODE_W,
                boxShadow: isRoot ? '0 8px 22px rgba(37,99,235,0.28)' : '0 2px 10px rgba(16,24,40,0.08)',
              }}
            >
              {!isRoot && (
                <span aria-hidden="true" style={{ width: 7, height: 7, borderRadius: 99, background: node.color, flexShrink: 0 }} />
              )}
              <span
                style={{
                  flex: 1,
                  minWidth: 0,
                  display: '-webkit-box',
                  WebkitBoxOrient: 'vertical',
                  WebkitLineClamp: 2,
                  overflow: 'hidden',
                  whiteSpace: 'normal',
                  overflowWrap: 'break-word',
                  lineHeight: 1.25,
                }}
              >
                {node.label}
              </span>
              {hasKids && (
                <button
                  type="button"
                  onPointerDown={(e) => e.stopPropagation()}
                  onClick={() => toggle(id)}
                  aria-expanded={!isCollapsed}
                  aria-label={`${isCollapsed ? 'Expand' : 'Collapse'} ${node.label || 'node'}`}
                  title={isCollapsed ? `Expand (${node.childIds.length})` : 'Collapse'}
                  style={{
                    marginLeft: 2,
                    minWidth: 20,
                    height: 20,
                    padding: '0 5px',
                    display: 'inline-flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    borderRadius: 99,
                    cursor: 'pointer',
                    flexShrink: 0,
                    fontSize: '0.72rem',
                    fontWeight: 700,
                    lineHeight: 1,
                    fontVariantNumeric: 'tabular-nums',
                    background: isRoot ? 'rgba(255,255,255,0.22)' : '#f1f3f5',
                    color: isRoot ? '#ffffff' : node.color,
                    border: `1px solid ${isRoot ? 'transparent' : MM_BORDER}`,
                  }}
                >
                  {isCollapsed ? `+${node.childIds.length}` : '−'}
                </button>
              )}
            </div>
          )
        })}
      </div>
      {(() => {
        if (!hovered || grabbing) return null
        const n = nodes[hovered]
        const p = positions[hovered]
        if (!n || !p) return null
        if (!n.detail && n.label.length <= 44) return null
        const sx = p.x * view.scale + view.x
        const sy = p.y * view.scale + view.y
        const lift = 34 * view.scale + 10
        const below = sy < 150
        return (
          <div
            style={{
              position: 'absolute',
              left: sx,
              top: below ? sy + lift : sy - lift,
              transform: below ? 'translate(-50%, 0)' : 'translate(-50%, -100%)',
              maxWidth: 300,
              padding: '8px 11px',
              borderRadius: 10,
              background: '#ffffff',
              border: `1px solid ${MM_BORDER}`,
              color: MM_TEXT,
              fontSize: '0.8rem',
              lineHeight: 1.4,
              whiteSpace: 'normal',
              overflowWrap: 'break-word',
              boxShadow: '0 10px 30px rgba(16,24,40,0.18)',
              pointerEvents: 'none',
              zIndex: 3,
            }}
          >
            {n.detail ? (
              <>
                <div style={{ fontWeight: 700, marginBottom: 3 }}>{n.label}</div>
                <div style={{ color: MM_MUTED }}>{n.detail}</div>
              </>
            ) : (
              n.label
            )}
          </div>
        )
      })()}
      <div style={{ position: 'absolute', top: 10, right: 10, display: 'flex', flexDirection: 'column', gap: 6, zIndex: 2 }}>
        {[
          { sym: '+', aria: 'Zoom in', on: zoomButton(1.2) },
          { sym: '−', aria: 'Zoom out', on: zoomButton(1 / 1.2) },
          { sym: '↺', aria: 'Reset view', on: centerView },
        ].map((b) => (
          <button
            key={b.aria}
            type="button"
            aria-label={b.aria}
            title={b.aria}
            onPointerDown={(e) => e.stopPropagation()}
            onClick={b.on}
            style={{
              width: 30,
              height: 30,
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              borderRadius: 8,
              cursor: 'pointer',
              fontSize: '1rem',
              fontWeight: 700,
              lineHeight: 1,
              color: MM_TEXT,
              background: '#ffffff',
              border: `1px solid ${MM_BORDER}`,
              boxShadow: '0 2px 8px rgba(16,24,40,0.1)',
            }}
          >
            {b.sym}
          </button>
        ))}
      </div>
      <div style={{ position: 'absolute', left: 12, bottom: 10, fontSize: '0.7rem', color: MM_MUTED, pointerEvents: 'none', userSelect: 'none' }}>
        Drag to pan · scroll to zoom · drag a node to move it
      </div>
    </div>
  )
}

export function Mindmap({ node, resolve }: NodeProps) {
  const root = (resolve(node.root) || {}) as MindmapData
  return <MindmapCanvas root={root} />
}

export function Unsupported({ node }: NodeProps) {
  return <div className="text-sm italic text-muted-foreground">Unsupported component: {asStr(node.component)}</div>
}

// Exported as `Card` for the registry; the local name avoids clashing with the
// shadcn Card primitive imported above.
export { Card_ as Card }
