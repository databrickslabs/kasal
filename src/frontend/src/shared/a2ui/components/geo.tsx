/**
 * Region-shaded and flow diagrams: `RegionHeatmap` and `Sankey`.
 *
 * Both are plain SVG with NO new npm dependency, for the reason stated in the
 * A2UI checklist: a dependency has to be declared in the exported app's
 * package.json too, and `test_a2ui_frontend_imports_are_declared_deps` fails
 * otherwise. Diagrams here follow the same rule.
 *
 * `RegionHeatmap` is NOT a choropleth and is deliberately not named one. A true
 * choropleth needs per-country boundary data (a TopoJSON per country, megabytes
 * each) which cannot be bundled for "any country the user names". This shades a
 * GRID of regions instead: it answers "which regions are high, which are low, by
 * how much" — the question a choropleth is usually asked — but it does NOT show
 * where they are. The honest name matters, because the catalog summary is what
 * the composer reads when deciding whether this fits the request.
 */
import { useCallback, useContext, useMemo } from 'react'
import type { NodeProps } from '../types'
import { DeckThemeContext, seriesFromAccent, readableTextOn } from '../lib/deckThemes'
import { asArr, asNum, asStr } from './values'

type Region = { name: string; value: number; label: string }
type Flow = { from: string; to: string; value: number }

/** Mix two hex colours. `t` 0 → a, 1 → b. Used for the intensity scale. */
function mix(a: string, b: string, t: number): string {
  const hex = (c: string) => {
    const s = c.replace('#', '')
    const n = s.length === 3 ? s.split('').map((x) => x + x).join('') : s
    return [parseInt(n.slice(0, 2), 16), parseInt(n.slice(2, 4), 16), parseInt(n.slice(4, 6), 16)]
  }
  const [r1, g1, b1] = hex(a)
  const [r2, g2, b2] = hex(b)
  const k = Math.max(0, Math.min(1, t))
  const ch = (x: number, y: number) => Math.round(x + (y - x) * k)
  return `#${[ch(r1, r2), ch(g1, g2), ch(b1, b2)].map((v) => v.toString(16).padStart(2, '0')).join('')}`
}

export function RegionHeatmap({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const title = asStr(resolve(node.title))
  const unit = asStr(resolve(node.unit))
  const regions: Region[] = useMemo(
    () =>
      asArr(resolve(node.regions))
        .map((r) => ({
          name: asStr(r?.name ?? r?.region ?? r?.label),
          value: asNum(r?.value) ?? 0,
          label: asStr(r?.label ?? r?.name ?? r?.region),
        }))
        .filter((r) => r.name),
    [resolve, node.regions],
  )
  if (!regions.length) return null

  const values = regions.map((r) => r.value)
  const max = Math.max(...values, 1)
  const min = Math.min(...values, 0)
  const span = max - min || 1
  // Shade from a barely-tinted panel to the full accent, so the ordering is
  // readable at slide distance without a legend lookup per cell.
  const shade = (v: number) => mix(theme.panel.startsWith('#') ? theme.panel : '#1a1a1a', theme.accent, (v - min) / span)

  // Near-square grid: read at slide size this wants compact rows, not a long
  // strip. 12 regions → 4x3, 28 → 6x5.
  const cols = Math.max(2, Math.min(6, Math.ceil(Math.sqrt(regions.length))))

  return (
    <div className="flex min-w-0 flex-col gap-3">
      {title && (
        <h3 className="text-base font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>
      )}
      <div
        className="grid gap-1.5"
        style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
      >
        {regions.map((r, i) => {
          const bg = shade(r.value)
          return (
            <div
              key={`${r.name}-${i}`}
              className="flex min-w-0 flex-col justify-between rounded p-2"
              style={{ background: bg, color: readableTextOn(bg), minHeight: 58 }}
              title={`${r.label}: ${r.value}${unit ? ` ${unit}` : ''}`}
            >
              <span className="truncate text-[0.7rem] font-semibold leading-tight">{r.label}</span>
              <span className="text-sm font-bold leading-none">{r.value}</span>
            </div>
          )
        })}
      </div>
      {/* Scale strip: without it the shading is ordinal but not quantitative. */}
      <div className="flex items-center gap-2 text-[0.7rem]" style={{ color: theme.muted }}>
        <span>{min}{unit ? ` ${unit}` : ''}</span>
        <div
          className="h-2 flex-1 rounded-full"
          style={{ background: `linear-gradient(to right, ${shade(min)}, ${shade(max)})` }}
        />
        <span>{max}{unit ? ` ${unit}` : ''}</span>
      </div>
    </div>
  )
}

export function Sankey({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const title = asStr(resolve(node.title))
  const unit = asStr(resolve(node.unit))
  const flows: Flow[] = useMemo(
    () =>
      asArr(resolve(node.flows))
        .map((f) => ({
          from: asStr(f?.from ?? f?.source),
          to: asStr(f?.to ?? f?.target),
          value: asNum(f?.value) ?? 0,
        }))
        .filter((f) => f.from && f.to && f.value > 0),
    [resolve, node.flows],
  )

  // Assign nodes to columns by longest path from a source, so a chain
  // (production → fuel → conversion → end use) lays out left to right without
  // the caller having to state depths. Cycles are broken by the depth cap.
  const layout = useMemo(() => {
    const names = Array.from(new Set(flows.flatMap((f) => [f.from, f.to])))
    const depth = new Map<string, number>(names.map((n) => [n, 0]))
    for (let pass = 0; pass < names.length; pass++) {
      let moved = false
      for (const f of flows) {
        const d = (depth.get(f.from) ?? 0) + 1
        if (d > (depth.get(f.to) ?? 0) && d < names.length) {
          depth.set(f.to, d)
          moved = true
        }
      }
      if (!moved) break
    }
    const maxDepth = Math.max(...Array.from(depth.values()), 0)
    const columns: string[][] = Array.from({ length: maxDepth + 1 }, () => [])
    names.forEach((n) => columns[depth.get(n) ?? 0].push(n))
    // A node's height is the larger of what flows in and what flows out, so
    // conservation is visible: a node that loses mass looks like it does.
    const weight = new Map<string, number>()
    names.forEach((n) => {
      const out = flows.filter((f) => f.from === n).reduce((s, f) => s + f.value, 0)
      const into = flows.filter((f) => f.to === n).reduce((s, f) => s + f.value, 0)
      weight.set(n, Math.max(out, into))
    })
    return { columns, weight, depth }
  }, [flows])

  const W = 900
  const H = 420
  const PAD = 8
  const NODE_W = 14
  const { columns, weight } = layout
  const colX = useCallback(
    (i: number) =>
      columns.length <= 1 ? PAD : PAD + (i * (W - NODE_W - PAD * 2)) / (columns.length - 1),
    [columns.length],
  )

  // Vertical placement per column, scaled so the tallest column fills the canvas.
  const geom = useMemo(() => {
    const box = new Map<string, { x: number; y: number; h: number }>()
    const colTotal = columns.map((c) => c.reduce((s, n) => s + (weight.get(n) ?? 0), 0))
    const maxTotal = Math.max(...colTotal, 1)
    const gap = 6
    columns.forEach((col, ci) => {
      const avail = H - PAD * 2 - gap * Math.max(col.length - 1, 0)
      const scale = avail / maxTotal
      let y = PAD
      col.forEach((n) => {
        const h = Math.max(3, (weight.get(n) ?? 0) * scale)
        box.set(n, { x: colX(ci), y, h })
        y += h + gap
      })
    })
    return box
  }, [columns, weight, colX])

  if (!flows.length) return null

  const palette = seriesFromAccent(theme.accent, Math.max(columns.flat().length, 1))
  const colorOf = (name: string) => palette[columns.flat().indexOf(name) % palette.length]

  // Stack ribbons at each endpoint so they leave and arrive without overlapping.
  const outCursor = new Map<string, number>()
  const inCursor = new Map<string, number>()
  const ribbons = flows.map((f, i) => {
    const a = geom.get(f.from)
    const b = geom.get(f.to)
    if (!a || !b) return null
    const aTotal = weight.get(f.from) || 1
    const bTotal = weight.get(f.to) || 1
    const ah = (f.value / aTotal) * a.h
    const bh = (f.value / bTotal) * b.h
    const ay = a.y + (outCursor.get(f.from) ?? 0)
    const by = b.y + (inCursor.get(f.to) ?? 0)
    outCursor.set(f.from, (outCursor.get(f.from) ?? 0) + ah)
    inCursor.set(f.to, (inCursor.get(f.to) ?? 0) + bh)
    const x1 = a.x + NODE_W
    const x2 = b.x
    const cx = (x1 + x2) / 2
    return (
      <path
        key={i}
        d={`M${x1},${ay} C${cx},${ay} ${cx},${by} ${x2},${by} L${x2},${by + bh} C${cx},${by + bh} ${cx},${ay + ah} ${x1},${ay + ah} Z`}
        fill={colorOf(f.from)}
        opacity={0.32}
      >
        <title>{`${f.from} → ${f.to}: ${f.value}${unit ? ` ${unit}` : ''}`}</title>
      </path>
    )
  })

  return (
    <div className="flex min-w-0 flex-col gap-2">
      {title && (
        <h3 className="text-base font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>
      )}
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 420 }} role="img">
        {ribbons}
        {columns.flat().map((n) => {
          const g = geom.get(n)
          if (!g) return null
          // Labels sit inside the canvas on the side with room, so a long name in
          // the last column is not clipped by the viewBox.
          const isLast = (layout.depth.get(n) ?? 0) === columns.length - 1
          return (
            <g key={n}>
              <rect x={g.x} y={g.y} width={NODE_W} height={g.h} fill={colorOf(n)} rx={2} />
              <text
                x={isLast ? g.x - 6 : g.x + NODE_W + 6}
                y={g.y + g.h / 2}
                textAnchor={isLast ? 'end' : 'start'}
                dominantBaseline="middle"
                fontSize={11}
                fill={theme.fg}
              >
                {n}
                <tspan fill={theme.muted}>{`  ${weight.get(n) ?? 0}${unit ? ` ${unit}` : ''}`}</tspan>
              </text>
            </g>
          )
        })}
      </svg>
    </div>
  )
}
