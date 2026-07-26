/**
 * SVG diagram renderers: Graph, Sequence and the Diagram archetypes.
 *
 * Drawn as plain SVG on purpose — no charting dependency to vendor into the
 * exported app.
 */
import { useContext, useMemo } from 'react'
import type { NodeProps } from '../types'
import { RotateCw } from 'lucide-react'
import { DeckThemeContext, seriesFromAccent, readableTextOn } from '../lib/deckThemes'
import type { DeckTheme } from '../lib/deckThemes'
import { asArr, asNum, asStr } from './values'

// ---- Graph (node-link diagram) -------------------------------------------
// A network / relationship graph drawn as dependency-free SVG. nodes is a list
// of {id, label?, group?, x?, y?}; edges a list of {from, to, label?}. Nodes are
// laid out on a circle (deterministic) unless a node carries explicit x/y. Group
// colors follow the workspace accent; directed edges get an arrowhead.
interface GNode {
  id: string
  label: string
  group?: string
  x?: number
  y?: number
}
export function Graph({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const rawNodes = asArr(resolve(node.nodes))
  const rawEdges = asArr(resolve(node.edges))
  const directed = node.directed !== false
  const { nodes, edges } = useMemo(() => {
    const nodes: GNode[] = rawNodes
      .filter((n) => n && typeof n === 'object')
      .map((n) => n as Record<string, unknown>)
      .map((n) => ({
        id: asStr(n.id ?? n.label),
        label: asStr(n.label ?? n.id),
        group: asStr(n.group) || undefined,
        x: asNum(n.x) ?? undefined,
        y: asNum(n.y) ?? undefined,
      }))
      .filter((n) => n.id)
    const ids = new Set(nodes.map((n) => n.id))
    const edges = rawEdges
      .filter((e) => e && typeof e === 'object')
      .map((e) => e as Record<string, unknown>)
      .map((e) => ({ from: asStr(e.from ?? e.source), to: asStr(e.to ?? e.target), label: asStr(e.label) || undefined }))
      .filter((e) => ids.has(e.from) && ids.has(e.to))
    return { nodes, edges }
  }, [rawNodes, rawEdges])

  if (!nodes.length) return null
  const W = 640
  const H = 400
  const cx = W / 2
  const cy = H / 2
  const R = Math.min(W, H) / 2 - 64
  const NR = 20
  const pos = new Map<string, { x: number; y: number }>()
  nodes.forEach((n, i) => {
    if (n.x != null && n.y != null) pos.set(n.id, { x: n.x, y: n.y })
    else {
      const a = (2 * Math.PI * i) / nodes.length - Math.PI / 2
      pos.set(n.id, { x: cx + R * Math.cos(a), y: cy + R * Math.sin(a) })
    }
  })
  const groups = Array.from(new Set(nodes.map((n) => n.group || '')))
  const colors = seriesFromAccent(theme.accent, Math.max(groups.length, 1))
  const colorOf = (n: GNode) => colors[Math.max(0, groups.indexOf(n.group || '')) % colors.length]
  // Trim an endpoint back to the node's rim so an arrowhead isn't hidden under it.
  const trim = (a: { x: number; y: number }, b: { x: number; y: number }, r: number) => {
    const dx = b.x - a.x
    const dy = b.y - a.y
    const len = Math.hypot(dx, dy) || 1
    return { x: b.x - (dx / len) * r, y: b.y - (dy / len) * r }
  }
  return (
    <div className="flex w-full min-w-0 flex-col">
      {node.title != null && <div className="mb-2 font-semibold">{asStr(node.title)}</div>}
      <div className="w-full overflow-x-auto rounded-lg border">
        <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" role="img" aria-label={asStr(node.title) || 'graph'}>
          <defs>
            <marker id="a2-graph-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
              <path d="M0,0 L10,5 L0,10 z" fill={theme.muted} />
            </marker>
          </defs>
          {edges.map((e, i) => {
            const a = pos.get(e.from) as { x: number; y: number }
            const b = pos.get(e.to) as { x: number; y: number }
            const end = trim(a, b, NR + 3)
            return (
              <g key={i}>
                <line
                  x1={a.x}
                  y1={a.y}
                  x2={end.x}
                  y2={end.y}
                  stroke={theme.muted}
                  strokeWidth={1.5}
                  markerEnd={directed ? 'url(#a2-graph-arrow)' : undefined}
                />
                {e.label && (
                  <text x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 - 4} textAnchor="middle" fontSize={10} fill={theme.muted}>
                    {e.label}
                  </text>
                )}
              </g>
            )
          })}
          {nodes.map((n) => {
            const p = pos.get(n.id) as { x: number; y: number }
            return (
              <g key={n.id}>
                <circle cx={p.x} cy={p.y} r={NR} fill={colorOf(n)} stroke={theme.bg} strokeWidth={2} />
                <text x={p.x} y={p.y + NR + 14} textAnchor="middle" fontSize={12} fill={theme.fg}>
                  {n.label}
                </text>
              </g>
            )
          })}
        </svg>
      </div>
    </div>
  )
}

// ---- Sequence diagram -----------------------------------------------------
// A UML-style sequence diagram in dependency-free SVG. actors is a list of
// names (or {id, label}); messages a list of {from, to, text?, dashed?} drawn
// top-to-bottom as arrows between actor lifelines. Actors referenced only in
// messages are backfilled in first-seen order.
export function Sequence({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const rawActors = asArr(resolve(node.actors))
  const rawMsgs = asArr(resolve(node.messages))
  const { actors, messages } = useMemo(() => {
    const norm = (a: unknown): { id: string; label: string } => {
      if (a && typeof a === 'object') {
        const o = a as Record<string, unknown>
        return { id: asStr(o.id ?? o.label ?? o.name), label: asStr(o.label ?? o.name ?? o.id) }
      }
      return { id: asStr(a), label: asStr(a) }
    }
    const actors = rawActors.map(norm).filter((a) => a.id)
    const messages = rawMsgs
      .filter((m) => m && typeof m === 'object')
      .map((m) => m as Record<string, unknown>)
      .map((m) => ({
        from: asStr(m.from ?? m.source),
        to: asStr(m.to ?? m.target),
        text: asStr(m.text ?? m.label ?? m.message),
        dashed: Boolean(m.dashed ?? m.return ?? m.async),
      }))
      .filter((m) => m.from && m.to)
    const known = new Set(actors.map((a) => a.id))
    for (const m of messages) {
      for (const id of [m.from, m.to]) {
        if (id && !known.has(id)) {
          known.add(id)
          actors.push({ id, label: id })
        }
      }
    }
    return { actors, messages }
  }, [rawActors, rawMsgs])

  if (!actors.length || !messages.length) return null
  const colW = 160
  const topH = 44
  const rowH = 48
  const padY = 26
  const W = Math.max(colW * actors.length, colW)
  const H = topH + padY + messages.length * rowH + padY
  const xOf = (id: string) => colW * (actors.findIndex((a) => a.id === id) + 0.5)
  const colors = seriesFromAccent(theme.accent, Math.max(actors.length, 1))
  return (
    <div className="flex w-full min-w-0 flex-col">
      {node.title != null && <div className="mb-2 font-semibold">{asStr(node.title)}</div>}
      <div className="w-full overflow-x-auto rounded-lg border">
        <svg viewBox={`0 0 ${W} ${H}`} width={W} height={H} role="img" aria-label={asStr(node.title) || 'sequence diagram'}>
          <defs>
            <marker id="a2-seq-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L10,5 L0,10 z" fill={theme.fg} />
            </marker>
          </defs>
          {actors.map((a, i) => (
            <g key={a.id}>
              <line x1={xOf(a.id)} y1={topH} x2={xOf(a.id)} y2={H - padY} stroke={theme.muted} strokeDasharray="4 4" strokeWidth={1} />
              <rect x={xOf(a.id) - colW / 2 + 10} y={8} width={colW - 20} height={topH - 14} rx={6} fill={colors[i % colors.length]} />
              <text x={xOf(a.id)} y={8 + (topH - 14) / 2 + 4} textAnchor="middle" fontSize={12} fill={theme.bg}>
                {a.label}
              </text>
            </g>
          ))}
          {messages.map((m, i) => {
            const y = topH + padY + i * rowH
            const x1 = xOf(m.from)
            const x2 = xOf(m.to)
            if (m.from === m.to) {
              return (
                <g key={i}>
                  <path
                    d={`M${x1},${y} h44 v20 h-44`}
                    fill="none"
                    stroke={theme.fg}
                    strokeWidth={1.4}
                    markerEnd="url(#a2-seq-arrow)"
                    strokeDasharray={m.dashed ? '5 4' : undefined}
                  />
                  {m.text && (
                    <text x={x1 + 52} y={y + 6} fontSize={11} fill={theme.fg}>
                      {m.text}
                    </text>
                  )}
                </g>
              )
            }
            return (
              <g key={i}>
                <line
                  x1={x1}
                  y1={y}
                  x2={x2}
                  y2={y}
                  stroke={theme.fg}
                  strokeWidth={1.4}
                  markerEnd="url(#a2-seq-arrow)"
                  strokeDasharray={m.dashed ? '5 4' : undefined}
                />
                {m.text && (
                  <text x={(x1 + x2) / 2} y={y - 6} textAnchor="middle" fontSize={11} fill={theme.fg}>
                    {m.text}
                  </text>
                )}
              </g>
            )
          })}
        </svg>
      </div>
    </div>
  )
}

// ---- Diagram (archetype-based business diagrams) ---------------------------
// Napkin-style: the composer CLASSIFIES the content into a curated visual
// archetype (process / timeline / cycle / funnel / pyramid / comparison /
// matrix2x2 / hierarchy) and supplies ONLY labels — the layout is deterministic,
// themed and dependency-free (HTML/CSS + inline SVG), so a weak model can never
// produce a broken drawing. Mirrored by a native-shape branch in the PPTX export.
export interface DiagramItem {
  label: string
  detail?: string
  value?: string
  points: string[]
  children: DiagramItem[]
}

export function normDiagramItems(v: unknown): DiagramItem[] {
  return asArr(v)
    .map((it): DiagramItem => {
      if (it && typeof it === 'object') {
        const o = it as Record<string, any>
        return {
          label: asStr(o.label ?? o.title ?? o.name ?? o.step ?? o.text),
          detail: asStr(o.detail ?? o.description ?? o.subtitle ?? o.date) || undefined,
          value: asStr(o.value) || undefined,
          points: asArr(o.points ?? o.bullets ?? o.items).map(asStr).filter(Boolean),
          children: normDiagramItems(o.children),
        }
      }
      return { label: asStr(it), points: [], children: [] }
    })
    .filter((it) => it.label)
}

// Model synonyms → the eight canonical archetypes (never render Unsupported for
// a near-miss like 'flow' or 'orgchart').
const ARCHETYPE_ALIASES: Record<string, string> = {
  process: 'process', flow: 'process', flowchart: 'process', steps: 'process', roadmap: 'timeline',
  timeline: 'timeline', milestones: 'timeline',
  cycle: 'cycle', loop: 'cycle',
  funnel: 'funnel', pipeline: 'funnel',
  pyramid: 'pyramid',
  comparison: 'comparison', versus: 'comparison', vs: 'comparison',
  matrix2x2: 'matrix2x2', matrix: 'matrix2x2', quadrant: 'matrix2x2',
  hierarchy: 'hierarchy', org: 'hierarchy', orgchart: 'hierarchy', tree: 'hierarchy',
}

function DiagramProcess({ items, colors }: { items: DiagramItem[]; colors: string[] }) {
  const notch = 16
  return (
    <div className="flex w-full items-stretch gap-1.5" role="list">
      {items.map((it, i) => {
        const c = colors[i % colors.length]
        const clip = i === 0
          ? `polygon(0 0, calc(100% - ${notch}px) 0, 100% 50%, calc(100% - ${notch}px) 100%, 0 100%)`
          : `polygon(0 0, calc(100% - ${notch}px) 0, 100% 50%, calc(100% - ${notch}px) 100%, 0 100%, ${notch}px 50%)`
        return (
          <div key={i} role="listitem" className="min-w-0 flex-1">
            <div
              className="flex h-full flex-col items-center justify-center px-5 py-3 text-center"
              style={{ background: c, color: readableTextOn(c), clipPath: clip }}
            >
              <div className="text-[0.65rem] font-bold uppercase tracking-wider opacity-80">Step {i + 1}</div>
              <div className="text-sm font-semibold leading-snug">{it.label}</div>
              {it.detail && <div className="mt-1 text-xs leading-snug opacity-85">{it.detail}</div>}
            </div>
          </div>
        )
      })}
    </div>
  )
}

function DiagramTimeline({ items, theme, colors }: { items: DiagramItem[]; theme: DeckTheme; colors: string[] }) {
  const n = items.length
  return (
    <div className="grid w-full" style={{ gridTemplateColumns: `repeat(${n}, minmax(0, 1fr))` }} role="list">
      {items.map((it, i) => (
        <div key={i} role="listitem" className="flex min-w-0 flex-col items-center px-1 text-center">
          <div className="relative flex w-full items-center justify-center py-2">
            {/* Rail segment — trimmed to start/end at the first/last dot. */}
            <div
              className="absolute top-1/2 h-0.5 -translate-y-1/2"
              style={{ background: theme.panelBorder, left: i === 0 ? '50%' : 0, right: i === n - 1 ? '50%' : 0 }}
            />
            <div className="relative z-[1] size-3.5 rounded-full border-2" style={{ background: colors[i % colors.length], borderColor: theme.bg }} />
          </div>
          <div className="text-sm font-semibold leading-snug" style={{ color: theme.fg }}>{it.label}</div>
          {it.detail && <div className="mt-0.5 text-xs leading-snug" style={{ color: theme.muted }}>{it.detail}</div>}
        </div>
      ))}
    </div>
  )
}

function DiagramCycle({ items, theme, colors }: { items: DiagramItem[]; theme: DeckTheme; colors: string[] }) {
  // Numbered pills on a circle around a central loop glyph; the numbering (plus
  // the glyph) carries the direction, so no distortion-prone SVG arc arrows.
  const n = items.length
  return (
    <div className="relative mx-auto w-full max-w-xl" style={{ height: 300 }}>
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2">
        <RotateCw className="size-10" style={{ color: theme.muted, opacity: 0.5 }} aria-hidden="true" />
      </div>
      {items.map((it, i) => {
        const a = (2 * Math.PI * i) / n - Math.PI / 2
        const c = colors[i % colors.length]
        return (
          <div
            key={i}
            className="absolute flex max-w-[180px] -translate-x-1/2 -translate-y-1/2 items-center gap-2 rounded-full border px-3 py-1.5"
            style={{
              left: `${50 + 40 * Math.cos(a)}%`,
              top: `${50 + 40 * Math.sin(a)}%`,
              background: theme.bg,
              backgroundImage: `linear-gradient(0deg, ${theme.panel}, ${theme.panel})`,
              borderColor: c,
              color: theme.fg,
            }}
            title={it.detail}
          >
            <span
              className="flex size-5 shrink-0 items-center justify-center rounded-full text-[0.7rem] font-bold"
              style={{ background: c, color: readableTextOn(c) }}
            >
              {i + 1}
            </span>
            <span className="truncate text-sm font-semibold">{it.label}</span>
          </div>
        )
      })}
    </div>
  )
}

function DiagramFunnel({ items, colors, invert = false }: { items: DiagramItem[]; colors: string[]; invert?: boolean }) {
  // invert=false → funnel (wide → narrow); invert=true → pyramid (apex → base).
  const n = Math.max(items.length - 1, 1)
  return (
    <div className="flex w-full flex-col items-center gap-1.5" role="list">
      {items.map((it, i) => {
        const pct = invert ? 42 + (i * 58) / n : 100 - (i * 55) / n
        const c = colors[i % colors.length]
        return (
          <div
            key={i}
            role="listitem"
            className="flex items-center justify-between gap-3 rounded-md px-5 py-2.5"
            style={{ width: `${pct}%`, background: c, color: readableTextOn(c) }}
          >
            <span className="min-w-0">
              <span className="block text-sm font-semibold leading-snug">{it.label}</span>
              {it.detail && <span className="block text-xs leading-snug opacity-85">{it.detail}</span>}
            </span>
            {it.value && <span className="shrink-0 text-sm font-bold">{it.value}</span>}
          </div>
        )
      })}
    </div>
  )
}

function DiagramComparison({ items, theme, colors }: { items: DiagramItem[]; theme: DeckTheme; colors: string[] }) {
  const cols = items.slice(0, 3)
  return (
    <div className="relative grid w-full gap-6" style={{ gridTemplateColumns: `repeat(${cols.length}, minmax(0, 1fr))` }}>
      {cols.map((it, i) => {
        const c = colors[i % colors.length]
        return (
          <div key={i} className="overflow-hidden rounded-xl border" style={{ borderColor: theme.panelBorder, background: theme.panel }}>
            <div className="px-4 py-2.5" style={{ background: c, color: readableTextOn(c) }}>
              <div className="text-sm font-bold leading-snug">{it.label}</div>
              {it.detail && <div className="text-xs leading-snug opacity-85">{it.detail}</div>}
            </div>
            <ul className="space-y-1.5 p-4 text-sm" style={{ color: theme.fg }}>
              {it.points.map((p, pi) => (
                <li key={pi} className="flex gap-2 leading-snug">
                  <span aria-hidden="true" className="mt-[7px] size-1.5 shrink-0 rounded-full" style={{ background: c }} />
                  {p}
                </li>
              ))}
            </ul>
          </div>
        )
      })}
      {cols.length === 2 && (
        <div
          className="absolute left-1/2 top-1/2 flex size-9 -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full border text-xs font-extrabold uppercase"
          style={{ background: theme.bg, borderColor: theme.panelBorder, color: theme.muted }}
          aria-hidden="true"
        >
          vs
        </div>
      )}
    </div>
  )
}

function DiagramMatrix({ items, theme, colors, xLabel, yLabel }: { items: DiagramItem[]; theme: DeckTheme; colors: string[]; xLabel?: string; yLabel?: string }) {
  const quads = items.slice(0, 4)
  return (
    <div className="flex w-full items-stretch gap-2">
      {yLabel && (
        <div className="flex shrink-0 items-center">
          <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: theme.muted, writingMode: 'vertical-rl', transform: 'rotate(180deg)' }}>
            {yLabel} →
          </span>
        </div>
      )}
      <div className="min-w-0 flex-1">
        <div className="grid grid-cols-2 gap-2">
          {quads.map((it, i) => {
            const c = colors[i % colors.length]
            return (
              <div key={i} className="rounded-lg border p-4" style={{ borderColor: theme.panelBorder, borderTop: `3px solid ${c}`, background: theme.panel }}>
                <div className="text-sm font-bold leading-snug" style={{ color: theme.fg }}>{it.label}</div>
                {it.detail && <div className="mt-1 text-xs leading-snug" style={{ color: theme.muted }}>{it.detail}</div>}
              </div>
            )
          })}
        </div>
        {xLabel && (
          <div className="mt-2 text-center text-xs font-semibold uppercase tracking-wider" style={{ color: theme.muted }}>
            {xLabel} →
          </div>
        )}
      </div>
    </div>
  )
}

function DiagramHierarchy({ items, theme, colors }: { items: DiagramItem[]; theme: DeckTheme; colors: string[] }) {
  // items[0] is the root; tolerate a flat list by treating the rest as children.
  const root = items[0]
  const children = root.children.length ? root.children : items.slice(1)
  return (
    <div className="flex w-full flex-col items-center">
      <div className="max-w-xs rounded-lg px-5 py-2.5 text-center" style={{ background: theme.accent, color: readableTextOn(theme.accent) }}>
        <div className="text-sm font-bold leading-snug">{root.label}</div>
        {root.detail && <div className="text-xs leading-snug opacity-85">{root.detail}</div>}
      </div>
      {children.length > 0 && (
        <>
          <div className="h-4 w-px" style={{ background: theme.panelBorder }} aria-hidden="true" />
          <div className="h-px w-4/5" style={{ background: theme.panelBorder }} aria-hidden="true" />
          <div className="grid w-full gap-3 pt-0" style={{ gridTemplateColumns: `repeat(${Math.min(children.length, 4)}, minmax(0, 1fr))` }}>
            {children.map((ch, i) => {
              const c = colors[i % colors.length]
              return (
                <div key={i} className="flex min-w-0 flex-col items-center">
                  <div className="h-3 w-px" style={{ background: theme.panelBorder }} aria-hidden="true" />
                  <div className="w-full rounded-lg border p-3 text-center" style={{ borderColor: theme.panelBorder, borderTop: `3px solid ${c}`, background: theme.panel }}>
                    <div className="text-sm font-semibold leading-snug" style={{ color: theme.fg }}>{ch.label}</div>
                    {ch.detail && <div className="mt-0.5 text-xs leading-snug" style={{ color: theme.muted }}>{ch.detail}</div>}
                    {ch.children.length > 0 && (
                      <ul className="mt-2 space-y-1 text-left text-xs" style={{ color: theme.muted }}>
                        {ch.children.map((g, gi) => (
                          <li key={gi} className="flex gap-1.5 leading-snug">
                            <span aria-hidden="true" className="mt-[5px] size-1 shrink-0 rounded-full" style={{ background: c }} />
                            {g.label}
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </>
      )}
    </div>
  )
}

export function Diagram({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const raw = asStr(node.archetype).toLowerCase().replace(/[\s_-]/g, '')
  const archetype = ARCHETYPE_ALIASES[raw] || 'process'
  const items = useMemo(() => normDiagramItems(resolve(node.items)), [resolve, node.items])
  if (!items.length) return null
  const colors = seriesFromAccent(theme.accent, Math.max(items.length, 2))
  const xLabel = asStr(node.xLabel) || undefined
  const yLabel = asStr(node.yLabel) || undefined
  const body =
    archetype === 'timeline' ? <DiagramTimeline items={items} theme={theme} colors={colors} />
    : archetype === 'cycle' ? <DiagramCycle items={items} theme={theme} colors={colors} />
    : archetype === 'funnel' ? <DiagramFunnel items={items} colors={colors} />
    : archetype === 'pyramid' ? <DiagramFunnel items={items} colors={colors} invert />
    : archetype === 'comparison' ? <DiagramComparison items={items} theme={theme} colors={colors} />
    : archetype === 'matrix2x2' ? <DiagramMatrix items={items} theme={theme} colors={colors} xLabel={xLabel} yLabel={yLabel} />
    : archetype === 'hierarchy' ? <DiagramHierarchy items={items} theme={theme} colors={colors} />
    : <DiagramProcess items={items} colors={colors} />
  return (
    <div className="flex w-full min-w-0 flex-col" aria-label={asStr(node.title) || `${archetype} diagram`}>
      {node.title != null && <div className="mb-3 font-semibold">{asStr(node.title)}</div>}
      {body}
    </div>
  )
}
