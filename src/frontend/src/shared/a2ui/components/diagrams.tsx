/**
 * SVG diagram renderers: Graph, Sequence and the Diagram archetypes.
 *
 * Drawn as plain SVG on purpose — no charting dependency to vendor into the
 * exported app.
 */
import { useContext, useMemo } from 'react'
import type { NodeProps } from '../types'
import { RotateCw } from 'lucide-react'
import { DeckThemeContext, seriesFromAccent } from '../lib/deckThemes'
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
