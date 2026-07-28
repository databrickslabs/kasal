/**
 * Mindmap surface: layout maths plus the pan/zoom canvas.
 */
import { useCallback, useContext, useMemo, useRef, useState } from 'react'
import type { NodeProps } from '../types'
import { DeckThemeContext, seriesFromAccent, readableTextOn } from '../lib/deckThemes'

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

function mindmapChildren(node: MindmapData): MindmapData[] {
  return Array.isArray(node.children)
    ? node.children.filter((c): c is MindmapData => Boolean(c) && typeof c === 'object')
    : []
}

// `accent` is the workspace accent (UIConfigurator source of truth): it seeds the
// root and the per-branch colors that descendants inherit, so the mindmap follows
// the brand instead of a fixed rainbow.
function buildMindmap(root: MindmapData, accent: string): { nodes: Record<string, MMNode>; rootId: string } {
  const nodes: Record<string, MMNode> = {}
  const branchColors = seriesFromAccent(accent, Math.max(mindmapChildren(root).length, 1))
  const walk = (node: MindmapData, id: string, depth: number, parentId: string | null, color: string) => {
    const kids = mindmapChildren(node)
    const childIds = kids.map((_, i) => `${id}.${i}`)
    const label = String(node.label ?? node.text ?? '')
    const explicit = node.description ?? node.detail ?? node.note
    const textVal = node.text != null ? String(node.text) : ''
    const detail = explicit != null ? String(explicit) : textVal && textVal !== label ? textVal : ''
    nodes[id] = { id, label, detail, depth, parentId, childIds, color }
    kids.forEach((k, i) => {
      const childColor = depth === 0 ? branchColors[i % branchColors.length] : color
      walk(k, childIds[i], depth + 1, id, childColor)
    })
  }
  walk(root, 'r', 0, null, accent)
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
  // Theme the canvas from the active deck palette (same source slides/quiz use)
  // so a picked palette colors the mindmap itself — not a backdrop behind it.
  // `theme.bg` is the solid palette background (stage is a gradient); nodes mirror
  // the slide stat-tile treatment (panel + panelBorder + fg).
  const theme = useContext(DeckThemeContext)
  const { nodes, rootId } = useMemo(() => buildMindmap(root, theme.accent), [root, theme.accent])
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
      className="a2-mindmap-canvas"
      onPointerDown={startPan}
      onPointerMove={onMove}
      onPointerUp={endDrag}
      onPointerLeave={endDrag}
      style={{
        position: 'relative',
        height: '64vh',
        minHeight: 460,
        overflow: 'hidden',
        // Square corners + the palette background so the canvas reads as one
        // continuous themed surface (no rounded edge revealing the page behind it).
        borderRadius: 0,
        border: `1px solid ${theme.panelBorder}`,
        background: theme.bg,
        color: theme.fg,
        cursor: grabbing ? 'grabbing' : 'grab',
        touchAction: 'none',
        backgroundImage: `radial-gradient(${theme.panelBorder} 1px, transparent 1px)`,
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
                // OPAQUE node fill (solid stage color + the panel tint on top) so
                // the connector lines drawn behind the node don't show THROUGH it —
                // `theme.panel` alone is semi-transparent on the built-in themes.
                backgroundColor: isRoot ? theme.accent : theme.bg,
                backgroundImage: isRoot ? 'none' : `linear-gradient(0deg, ${theme.panel}, ${theme.panel})`,
                color: isRoot ? readableTextOn(theme.accent) : theme.fg,
                border: `1px solid ${isRoot ? theme.accent : theme.panelBorder}`,
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
                    background: isRoot ? 'rgba(255,255,255,0.22)' : 'rgba(127,127,127,0.18)',
                    color: isRoot ? readableTextOn(theme.accent) : node.color,
                    border: `1px solid ${isRoot ? 'transparent' : theme.panelBorder}`,
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
              background: theme.bg,
              border: `1px solid ${theme.panelBorder}`,
              color: theme.fg,
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
                <div style={{ color: theme.muted }}>{n.detail}</div>
              </>
            ) : (
              n.label
            )}
          </div>
        )
      })()}
      <div style={{ position: 'absolute', top: 10, left: 10, display: 'flex', flexDirection: 'column', gap: 6, zIndex: 2 }}>
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
              color: theme.fg,
              background: theme.panel,
              border: `1px solid ${theme.panelBorder}`,
              boxShadow: '0 2px 8px rgba(16,24,40,0.1)',
            }}
          >
            {b.sym}
          </button>
        ))}
      </div>
      <div style={{ position: 'absolute', left: 12, bottom: 10, fontSize: '0.7rem', color: theme.muted, pointerEvents: 'none', userSelect: 'none' }}>
        Drag to pan · scroll to zoom · drag a node to move it
      </div>
    </div>
  )
}

export function Mindmap({ node, resolve }: NodeProps) {
  const root = (resolve(node.root) || {}) as MindmapData
  return <MindmapCanvas root={root} />
}
