import { createContext, Fragment, lazy, Suspense, useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import {
  ResponsiveContainer,
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, ComposedChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { ComponentNode, NodeProps } from './types'
import { Check, ChevronDown, ChevronLeft, ChevronRight, Download, FileText, Lightbulb, Presentation, RotateCcw, RotateCw, Shuffle, Trophy, X } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from './ui/card'
import { Separator } from './ui/separator'
import { Button } from './ui/button'
import { downloadCsv, downloadPptx } from './lib/download'
import { DeckThemeContext, deckProseVars, seriesFromAccent, readableTextOn } from './lib/deckThemes'
import { SurfaceContext, SurfaceChromeContext } from './lib/surfaceContext'
import { mdComponents, linkifyCitations } from './lib/markdown'
import { cn } from './lib/utils'

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
    // Big-number stat tile, themed to the active deck. `h-full` + flex column so a
    // row of tiles is always equal height (a longer label wrapping to two lines no
    // longer makes its tile taller than its neighbours).
    return (
      <div className="flex h-full flex-col rounded-xl border p-5" style={{ background: theme.panel, borderColor: theme.panelBorder }}>
        <div className="text-[2.2rem] font-extrabold leading-none" style={{ color: theme.accent }}>
          {asStr(resolve(node.value))}
        </div>
        {/* Label uses the body foreground (not `muted`) so it stays legible — a
            workspace palette whose muted color sits near the surface color would
            otherwise wash the label out against the tile. */}
        <div className="mt-2 text-sm font-medium" style={{ color: theme.fg, opacity: 0.85 }}>{asStr(resolve(node.label))}</div>
      </div>
    )
  }
  return (
    <div className="flex h-full flex-col rounded-xl border bg-secondary/40 p-4">
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

// Parse a cell for numeric-aware sorting: strips the decorations models emit on
// figures (~ approx, thousands commas, currency, %, whitespace). Returns a number
// when the cleaned value is numeric, else null so the caller falls back to a
// locale string compare. Lets a "Total DBUs" column of "~905,930" sort as numbers.
const numericValue = (s: string): number | null => {
  const cleaned = s.replace(/[~,$%\s]/g, '')
  if (!/^-?\d*\.?\d+$/.test(cleaned)) return null
  const n = Number(cleaned)
  return Number.isFinite(n) ? n : null
}

export function Table({ node, resolve }: NodeProps) {
  // Resolve columns through the dataModel too — the composer may bind it
  // (`columns: {"path":"/cols"}`), not just pass a literal array; without resolve
  // a bound header came back empty (no <thead>).
  const columns = asArr(resolve(node.columns))
  const rawRows = asArr(resolve(node.rows))
  const { downloads: showDownloads } = useContext(SurfaceChromeContext)

  // Click a header to cycle asc → desc → original order. Numeric-aware so figure
  // columns sort by magnitude, not lexically. null = document order.
  const [sort, setSort] = useState<{ col: number; dir: 'asc' | 'desc' } | null>(null)
  // A substring filter across all cells — shown only for tables large enough to
  // benefit (small ones read fine as-is; sorting stays available on every table).
  const [query, setQuery] = useState('')
  const searchable = rawRows.length > 8

  const cell = useCallback((row: unknown, ci: number) => asStr(asArr(row)[ci]), [])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return rawRows
    return rawRows.filter((row) => asArr(row).some((c) => asStr(c).toLowerCase().includes(q)))
  }, [rawRows, query])

  const sorted = useMemo(() => {
    if (!sort) return filtered
    const { col, dir } = sort
    const factor = dir === 'asc' ? 1 : -1
    // Copy before sorting (never mutate the resolved dataModel array).
    return [...filtered].sort((a, b) => {
      const sa = cell(a, col)
      const sb = cell(b, col)
      const na = numericValue(sa)
      const nb = numericValue(sb)
      if (na != null && nb != null) return (na - nb) * factor
      return sa.localeCompare(sb, undefined, { numeric: true }) * factor
    })
  }, [filtered, sort, cell])

  const cycleSort = (col: number) =>
    setSort((prev) => {
      if (!prev || prev.col !== col) return { col, dir: 'asc' }
      return prev.dir === 'asc' ? { col, dir: 'desc' } : null
    })

  return (
    <div>
      {(showDownloads || searchable) && (
        <div className="mb-1 flex items-center justify-between gap-2">
          {showDownloads ? (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 gap-1 px-2 text-xs text-muted-foreground"
              // Export the current VIEW (sorted + filtered) — what the user sees.
              onClick={() => downloadCsv(columns.map(asStr), sorted.map((r) => asArr(r).map(asStr)), 'table.csv')}
            >
              <Download className="size-3.5" /> CSV
            </Button>
          ) : (
            <span />
          )}
          {searchable && (
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Filter rows…"
              aria-label="Filter table rows"
              className="h-7 w-40 rounded-md border bg-transparent px-2 text-xs outline-none focus:ring-1 focus:ring-ring"
            />
          )}
        </div>
      )}
      <div className="overflow-x-auto rounded-lg border">
      <table className="w-full border-collapse text-sm">
        {columns.length > 0 && (
          <thead>
            <tr>
              {/* Inverted header (foreground bg / background text) — a guaranteed
                  contrasting pair from the palette. `bg-muted` (= surface) could
                  collide with the foreground text on palettes whose surface is
                  dark, leaving the header unreadable. Each header is a sort toggle. */}
              {columns.map((c, i) => {
                const active = sort?.col === i
                return (
                  <th key={i} className="bg-foreground px-3 py-2 text-left font-semibold text-background">
                    <button
                      type="button"
                      onClick={() => cycleSort(i)}
                      aria-label={`Sort by ${asStr(c)}`}
                      className="flex cursor-pointer select-none items-center gap-1 hover:opacity-80"
                    >
                      {asStr(c)}
                      <span className="text-[10px] opacity-70">{active ? (sort!.dir === 'asc' ? '▲' : '▼') : '↕'}</span>
                    </button>
                  </th>
                )
              })}
            </tr>
          </thead>
        )}
        <tbody>
          {sorted.map((row, ri) => (
            <tr key={ri} className={cn('border-t', ri % 2 === 1 && 'bg-muted/30')}>
              {asArr(row).map((cell, ci) => (
                <td key={ci} className="px-3 py-2">{asStr(cell)}</td>
              ))}
            </tr>
          ))}
          {sorted.length === 0 && (
            <tr>
              <td colSpan={Math.max(1, columns.length)} className="px-3 py-6 text-center text-muted-foreground">
                No matching rows
              </td>
            </tr>
          )}
        </tbody>
      </table>
      </div>
      {searchable && query.trim() !== '' && (
        <div className="mt-1 text-xs text-muted-foreground">
          {sorted.length} of {rawRows.length} rows
        </div>
      )}
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

// One height for EVERY chart type so charts sitting in the same dashboard row
// line up (symmetry). Pie gets a relative radius + a margin band so its labels /
// leader lines and the bottom legend always fit inside the box — never overflowing
// upward into the title above it.
const CHART_HEIGHT = 300

export function Chart({ node, resolve }: NodeProps) {
  const type = asStr(node.chartType) || 'bar'
  const data = asArr(resolve(node.data))
  const xKey = asStr(node.xKey) || 'name'
  const yKeys = asArr(node.yKeys).map(asStr)
  const keys = yKeys.length ? yKeys : ['value']
  // Series colors follow the workspace accent (UIConfigurator is the source of
  // truth) instead of a fixed rainbow: first color IS the accent, the rest are
  // evenly-spaced hues derived from it. Pie needs one per slice, bar/line one
  // per series.
  const theme = useContext(DeckThemeContext)
  const series = seriesFromAccent(theme.accent, Math.max(data.length, keys.length, 1))
  return (
    <div className="flex h-full w-full min-w-0 flex-col">
      {node.title != null && <div className="mb-2 font-semibold">{asStr(node.title)}</div>}
      {/* Fixed-height box keeps the SVG bounded so it can't bleed into the title. */}
      <div style={{ width: '100%', height: CHART_HEIGHT }}>
      <ResponsiveContainer width="100%" height="100%">
        {type === 'pie' ? (
          <PieChart margin={{ top: 8, right: 16, bottom: 8, left: 16 }}>
            {/* Radius RELATIVE to the box and cy lifted to 46% so the bottom legend
                has room — a fixed 90px radius overflowed small cells. */}
            <Pie data={data} dataKey={keys[0]} nameKey={xKey} cx="50%" cy="46%" outerRadius="70%" label>
              {data.map((_, i) => <Cell key={i} fill={series[i % series.length]} />)}
            </Pie>
            <Tooltip /><Legend />
          </PieChart>
        ) : type === 'line' ? (
          <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
            {keys.map((k, i) => <Line key={k} type="monotone" dataKey={k} stroke={series[i % series.length]} />)}
          </LineChart>
        ) : (
          <BarChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
            {keys.map((k, i) => <Bar key={k} dataKey={k} fill={series[i % series.length]} />)}
          </BarChart>
        )}
      </ResponsiveContainer>
      </div>
    </div>
  )
}

// Coerce a value to a finite number, or null. Tolerates numeric strings with
// thousands separators / units (e.g. "1,234", "12%") that models often emit.
const asNum = (v: unknown): number | null => {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v.replace(/[,\s%$]/g, ''))
    return Number.isFinite(n) ? n : null
  }
  return null
}

// ---- Forecast -------------------------------------------------------------
// A time-series forecast: a forecast line + an optional shaded confidence band
// (lower/upper) + optional historical actuals, split into one series per
// category when the data is long-format.
//
// Deliberately SCHEMA-AGNOSTIC: a forecasting query returns wildly different
// column names per use case (ds/date/period; yhat/forecast/prediction/mean;
// *_lower/*_upper/ci_*; an optional category column to split series). We INFER
// each role from the first row's columns (name patterns + numeric detection);
// any explicit `xKey/forecastKey/lowerKey/upperKey/actualKey/seriesKey` prop on
// the node overrides the guess. So it renders the same whether the data is
// {ds, risk_category, default_rate_forecast, ...} or {month, yhat, yhat_lo, ...}.
interface ForecastSpec {
  xKey: string
  seriesKey?: string
  forecastKey: string
  lowerKey?: string
  upperKey?: string
  actualKey?: string
}

function inferForecastSpec(rows: Record<string, unknown>[], node: ComponentNode): ForecastSpec {
  const first = rows.find((r) => r && typeof r === 'object') || {}
  const keys = Object.keys(first)
  const lc = (k: string) => k.toLowerCase()
  const isNumericCol = (k: string) => {
    let num = 0
    let tot = 0
    for (const r of rows.slice(0, 20)) {
      if (r && typeof r === 'object' && k in r) {
        tot++
        if (asNum((r as Record<string, unknown>)[k]) != null) num++
      }
    }
    return tot > 0 && num / tot >= 0.7
  }
  const numeric = keys.filter(isNumericCol)
  const nonNumeric = keys.filter((k) => !numeric.includes(k))
  const pick = (pool: string[], pats: string[]) => pool.find((k) => pats.some((p) => lc(k).includes(p)))
  const distinct = (k: string) => new Set(rows.map((r) => asStr((r as Record<string, unknown>)[k]))).size

  const xKey =
    asStr(node.xKey) ||
    pick(nonNumeric, ['date', 'time', 'period', 'month', 'week', 'day', 'timestamp', 'quarter', 'year', 'ds']) ||
    (nonNumeric.length ? [...nonNumeric].sort((a, b) => distinct(b) - distinct(a))[0] : keys[0]) ||
    'x'
  const lowerKey = asStr(node.lowerKey) || pick(numeric, ['lower', '_lo', '_min', 'ci_low', 'low', 'p05', 'q05', 'floor'])
  const upperKey = asStr(node.upperKey) || pick(numeric, ['upper', '_hi', '_max', 'ci_high', 'high', 'p95', 'q95', 'cap'])
  const actualKey =
    asStr(node.actualKey) ||
    pick(numeric.filter((k) => k !== lowerKey && k !== upperKey), ['actual', 'observed', 'history', 'y_true', 'truth'])
  const usedNum = new Set([lowerKey, upperKey, actualKey].filter(Boolean) as string[])
  const forecastKey =
    asStr(node.forecastKey) ||
    asStr(node.yKey) ||
    pick(numeric, ['forecast', 'predict', 'yhat', 'mean', 'expected', 'estimate']) ||
    numeric.find((k) => !usedNum.has(k)) ||
    numeric[0] ||
    'value'
  const seriesKey =
    asStr(node.seriesKey) || nonNumeric.find((k) => k !== xKey && distinct(k) > 1 && distinct(k) < rows.length)
  return {
    xKey,
    seriesKey: seriesKey || undefined,
    forecastKey,
    lowerKey: lowerKey || undefined,
    upperKey: upperKey || undefined,
    actualKey: actualKey || undefined,
  }
}

export function Forecast({ node, resolve }: NodeProps) {
  const rows = asArr(resolve(node.data)).filter((r) => r && typeof r === 'object') as Record<string, unknown>[]
  const theme = useContext(DeckThemeContext)
  const spec = useMemo(() => inferForecastSpec(rows, node), [rows, node])

  const { data, seriesList, hasBand } = useMemo(() => {
    const { xKey, seriesKey, forecastKey, lowerKey, upperKey, actualKey } = spec
    const hasBand = Boolean(lowerKey && upperKey)
    const band = (r: Record<string, unknown>): [number, number] | undefined => {
      if (!hasBand) return undefined
      const lo = asNum(r[lowerKey as string])
      const up = asNum(r[upperKey as string])
      return lo != null && up != null ? [lo, up] : undefined
    }
    if (seriesKey) {
      const byX = new Map<string, Record<string, unknown>>()
      const seen: string[] = []
      for (const r of rows) {
        const x = asStr(r[xKey])
        const s = asStr(r[seriesKey]) || 'series'
        if (!seen.includes(s)) seen.push(s)
        const row = byX.get(x) ?? { [xKey]: x }
        const f = asNum(r[forecastKey])
        if (f != null) row[s] = f
        const b = band(r)
        if (b) row[`${s}__band`] = b
        if (actualKey) {
          const a = asNum(r[actualKey])
          if (a != null) row[`${s}__actual`] = a
        }
        byX.set(x, row)
      }
      return { data: Array.from(byX.values()), seriesList: seen, hasBand }
    }
    const data = rows.map((r) => {
      const row: Record<string, unknown> = { [xKey]: asStr(r[xKey]) }
      const f = asNum(r[forecastKey])
      if (f != null) row.forecast = f
      const b = band(r)
      if (b) row.forecast__band = b
      if (actualKey) {
        const a = asNum(r[actualKey])
        if (a != null) row.actual = a
      }
      return row
    })
    return { data, seriesList: ['forecast'], hasBand }
  }, [rows, spec])

  if (!data.length) return null
  const colors = seriesFromAccent(theme.accent, Math.max(seriesList.length, 1))
  const hasActual = Boolean(spec.actualKey)
  const single = seriesList.length === 1 && seriesList[0] === 'forecast'
  return (
    <div className="flex h-full w-full min-w-0 flex-col">
      {node.title != null && <div className="mb-2 font-semibold">{asStr(node.title)}</div>}
      <div style={{ width: '100%', height: CHART_HEIGHT }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis dataKey={spec.xKey} />
            <YAxis />
            <Tooltip />
            <Legend />
            {seriesList.map((s, i) => {
              const c = colors[i % colors.length]
              const actualKey = single ? 'actual' : `${s}__actual`
              const bandKey = single ? 'forecast__band' : `${s}__band`
              return (
                <Fragment key={s}>
                  {hasBand && (
                    <Area
                      dataKey={bandKey}
                      stroke="none"
                      fill={c}
                      fillOpacity={0.14}
                      legendType="none"
                      isAnimationActive={false}
                      connectNulls
                    />
                  )}
                  {hasActual && (
                    <Line
                      dataKey={actualKey}
                      name={single ? 'Actual' : `${s} (actual)`}
                      stroke={c}
                      dot={false}
                      strokeWidth={2}
                      isAnimationActive={false}
                      connectNulls
                    />
                  )}
                  <Line
                    dataKey={s}
                    name={hasActual ? (single ? 'Forecast' : `${s} (forecast)`) : s === 'forecast' ? 'Forecast' : s}
                    stroke={c}
                    dot={false}
                    strokeWidth={2}
                    strokeDasharray={hasActual ? '5 4' : undefined}
                    isAnimationActive={false}
                    connectNulls
                  />
                </Fragment>
              )
            })}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

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

// Slide layout context: which slide is showing + that we're inside a deck (so
// KeyValue renders as a themed stat tile). The deck THEME comes from
// DeckThemeContext (one theme for the whole deck — variety is by LAYOUT below).
const SlideCtx = createContext<{ idx: number; total: number; inDeck: boolean }>({
  idx: 0,
  total: 1,
  inDeck: false,
})

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

export function Slide({ node, render }: NodeProps) {
  const { idx, total } = useContext(SlideCtx)
  const theme = useContext(DeckThemeContext)
  const surface = useContext(SurfaceContext)
  const variant = (asStr(node.variant) || 'content').toLowerCase()
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

  // A 'content' slide that ended up with a title but NO body would render as a
  // title stranded over a big empty void — a broken-looking near-empty slide.
  // Redirect it to the centered SECTION layout so the lone title reads as a
  // deliberate divider regardless of what the generator emitted.
  const titleOnlyContent = variant === 'content' && !slideHasBody && node.title != null
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

// FNV-1a hash of a string -> uint32 seed (stable, dependency-free).
function hashStr(s: string): number {
  let h = 2166136261
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return h >>> 0
}

// A deterministic permutation of [0..n-1] from a seed (seeded Fisher-Yates with a
// small LCG). The same seed always yields the same order, so calling it inline on
// every render is stable — no hook / state needed.
function shuffleIndices(n: number, seed: number): number[] {
  const idx = Array.from({ length: n }, (_, i) => i)
  let s = (seed || 1) >>> 0
  for (let i = n - 1; i > 0; i--) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0
    const j = s % (i + 1)
    const t = idx[i]
    idx[i] = idx[j]
    idx[j] = t
  }
  return idx
}

// ---- Quiz (interactive multiple-choice assessment) -------------------------
// One question at a time with Prev/Next + progress dots (mirrors SlideDeck), an
// immediate right/wrong reveal on select, and a final score summary. Self-grades
// from each question's `answer` index — the composer supplies ONLY the data.
type QuizQuestion = {
  question?: unknown
  options?: unknown
  answer?: unknown
  explanation?: unknown
}

export function Quiz({ node, resolve }: NodeProps) {
  const title = asStr(resolve(node.title))
  const questions = asArr(resolve(node.questions)) as QuizQuestion[]
  const total = questions.length
  // idx ranges over [0, total]; idx === total is the results summary (the deck's
  // "closing slide" analogue). Hooks run before the empty-guard so order is stable.
  const [idx, setIdx] = useState(0)
  const [picked, setPicked] = useState<Record<number, number>>({})
  // Themed like a slide deck: the active theme flows in via DeckThemeContext from
  // the QuizSurface picker (App.tsx). Hook runs before the empty-guard so the hook
  // order is stable.
  const theme = useContext(DeckThemeContext)
  if (!total) return null

  const clamp = (n: number) => Math.max(0, Math.min(total, n))
  const correctOf = (i: number) => Number(questions[i]?.answer)
  const score = questions.reduce(
    (acc, q, i) => acc + (picked[i] === Number(q.answer) ? 1 : 0),
    0,
  )
  const onResults = idx >= total

  const OK = '#10b981'
  const BAD = '#ef4444'

  // A slim progress meter that tracks how far through the quiz the user is — a
  // staple of the quiz "feel" (1-indexed so question 1 already reads as progress).
  const progressPct = onResults ? 100 : Math.round(((idx + 1) / total) * 100)
  const progress = (
    <div className="h-2 w-full overflow-hidden rounded-full" style={{ background: theme.panel }}>
      <div
        className="h-full rounded-full transition-all duration-500 ease-out"
        style={{ width: `${progressPct}%`, background: theme.accent }}
      />
    </div>
  )

  // Navigation dots double as an answer key: green = answered correctly, red =
  // answered wrong, muted = unseen. The current question gets an accent pill.
  const dots = (
    <div className="flex flex-wrap items-center justify-center gap-1.5">
      {Array.from({ length: total + 1 }).map((_, i) => {
        const isCur = i === idx
        let bg = theme.muted
        let op = 0.4
        if (i === total) {
          bg = theme.accent
          op = 0.9
        } else if (picked[i] != null) {
          bg = picked[i] === correctOf(i) ? OK : BAD
          op = 0.95
        }
        return (
          <button
            key={i}
            aria-label={i === total ? 'Results' : `Go to question ${i + 1}`}
            onClick={() => setIdx(i)}
            className="h-2 rounded-full transition-all"
            style={isCur ? { width: 22, background: theme.accent } : { width: 8, background: bg, opacity: op }}
          />
        )
      })}
    </div>
  )

  const nav = (
    <div className="flex items-center justify-between gap-3">
      <Button variant="outline" size="sm" className="gap-1" onClick={() => setIdx((i) => clamp(i - 1))} disabled={idx === 0}>
        <ChevronLeft className="size-4" /> Prev
      </Button>
      {dots}
      <Button variant="outline" size="sm" className="gap-1" onClick={() => setIdx((i) => clamp(i + 1))} disabled={idx >= total}>
        Next <ChevronRight className="size-4" />
      </Button>
    </div>
  )

  if (onResults) {
    const pct = Math.round((score / total) * 100)
    const grade =
      pct >= 90 ? 'Outstanding!' : pct >= 75 ? 'Great job!' : pct >= 50 ? 'Good effort!' : 'Keep practicing!'
    // Circular score ring: an SVG donut whose accent arc sweeps to `pct`.
    const radius = 54
    const circ = 2 * Math.PI * radius
    return (
      <div className="flex flex-col gap-4">
        {title && <h3 className="text-lg font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>}
        {progress}
        <div
          className="flex flex-col items-center gap-3 rounded-2xl border p-8 text-center"
          style={{ background: theme.stage, borderColor: theme.panelBorder, color: theme.fg }}
        >
          <Trophy className="size-7" style={{ color: theme.accent }} />
          <div className="text-xl font-bold tracking-tight" style={{ color: theme.title }}>{grade}</div>
          <div className="relative" style={{ width: 128, height: 128 }}>
            <svg width="128" height="128" className="-rotate-90">
              <circle cx="64" cy="64" r={radius} fill="none" stroke={theme.panel} strokeWidth="10" />
              <circle
                cx="64"
                cy="64"
                r={radius}
                fill="none"
                stroke={theme.accent}
                strokeWidth="10"
                strokeLinecap="round"
                strokeDasharray={circ}
                strokeDashoffset={circ - circ * (pct / 100)}
                style={{ transition: 'stroke-dashoffset 0.8s ease' }}
              />
            </svg>
            <div className="absolute inset-0 flex flex-col items-center justify-center">
              <div className="text-3xl font-extrabold leading-none" style={{ color: theme.accent }}>{pct}%</div>
              <div className="mt-1 text-xs font-medium" style={{ color: theme.muted }}>{score} / {total} correct</div>
            </div>
          </div>
          <Button
            variant="outline"
            size="sm"
            className="mt-1 gap-1"
            onClick={() => {
              setPicked({})
              setIdx(0)
            }}
          >
            <RotateCcw className="size-3.5" /> Retake quiz
          </Button>
        </div>
        {nav}
      </div>
    )
  }

  const q = questions[idx] || {}
  const options = asArr(q.options)
  const correct = Number(q.answer)
  const chosen = picked[idx]
  const answered = chosen != null
  const isRight = answered && chosen === correct
  const explanation = asStr(q.explanation)
  // Display options in a deterministic, per-question shuffled order so the correct
  // answer isn't always in the same slot (composer models tend to park it at a
  // fixed index, e.g. always the 2nd option). Seeded by the question text → stable
  // across re-renders and navigation (never reshuffles under the user) yet varied
  // question-to-question. `picked` and scoring keep the ORIGINAL option index, so
  // grading is unaffected.
  const order = shuffleIndices(options.length, hashStr(asStr(q.question)))

  return (
    <div className="flex flex-col gap-4">
      {title && <h3 className="text-lg font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>}
      {progress}
      <div className="rounded-2xl border p-6" style={{ background: theme.stage, borderColor: theme.panelBorder, color: theme.fg }}>
        <div className="flex items-center justify-between gap-3">
          <span
            className="inline-flex items-center rounded-full px-3 py-1 text-xs font-bold uppercase tracking-wide"
            style={{ background: theme.panel, color: theme.accent }}
          >
            Question {idx + 1} of {total}
          </span>
          {answered && (
            <span className="inline-flex items-center gap-1 text-xs font-bold" style={{ color: isRight ? OK : BAD }}>
              {isRight ? <Check className="size-3.5" /> : <X className="size-3.5" />}
              {isRight ? 'Correct' : 'Incorrect'}
            </span>
          )}
        </div>
        <p className="mt-3 text-base font-semibold leading-snug" style={{ color: theme.fg }}>{asStr(q.question)}</p>
        <div className="mt-4 flex flex-col gap-2.5">
          {order.map((oi, pos) => {
            const opt = options[oi]
            const isCorrect = oi === correct
            const isChosen = oi === chosen
            const showCorrect = answered && isCorrect
            const showWrong = answered && isChosen && !isCorrect
            const dim = answered && !isCorrect && !isChosen
            const letter = String.fromCharCode(65 + pos)
            // Themed until answered; on reveal, semantic green/red overrides the
            // theme so right/wrong reads clearly on any palette.
            const cardStyle = showCorrect
              ? { borderColor: OK, background: 'rgba(16,185,129,0.12)', color: theme.fg }
              : showWrong
                ? { borderColor: BAD, background: 'rgba(239,68,68,0.12)', color: theme.fg }
                : { borderColor: theme.panelBorder, color: theme.fg, opacity: dim ? 0.55 : 1 }
            // The A/B/C/D badge turns into a green check / red cross on reveal.
            const badgeStyle = showCorrect
              ? { background: OK, color: '#fff', borderColor: OK }
              : showWrong
                ? { background: BAD, color: '#fff', borderColor: BAD }
                : { background: theme.panel, color: theme.fg, borderColor: theme.panelBorder }
            return (
              <button
                key={oi}
                disabled={answered}
                onClick={() => setPicked((p) => ({ ...p, [idx]: oi }))}
                className={cn(
                  'flex items-center gap-3 rounded-xl border px-4 py-3.5 text-left text-sm font-medium transition-all',
                  !answered && 'hover:-translate-y-0.5 hover:shadow-md',
                )}
                style={cardStyle}
              >
                <span
                  className="flex size-7 shrink-0 items-center justify-center rounded-full border text-xs font-bold"
                  style={badgeStyle}
                >
                  {showCorrect ? <Check className="size-4" /> : showWrong ? <X className="size-4" /> : letter}
                </span>
                <span className="flex-1">{asStr(opt)}</span>
              </button>
            )
          })}
        </div>
        {answered && explanation && (
          <div
            className="mt-4 flex items-start gap-2 rounded-lg px-3 py-2.5 text-sm"
            style={{ background: theme.panel, color: theme.muted }}
          >
            <Lightbulb className="mt-0.5 size-4 shrink-0" style={{ color: theme.accent }} />
            <span>{explanation}</span>
          </div>
        )}
      </div>
      {nav}
    </div>
  )
}

// ---- Flashcards (Anki-style study deck) ------------------------------------
// Flippable cards (click to reveal the back), Prev/Next deck navigation, a
// deterministic shuffle, and a "known" tally — themed like the Quiz/SlideDeck.
type FlashCard = {
  front?: unknown
  back?: unknown
  hint?: unknown
}

export function Flashcards({ node, resolve }: NodeProps) {
  const title = asStr(resolve(node.title))
  const cards = asArr(resolve(node.cards)) as FlashCard[]
  const total = cards.length
  // Hooks run before the empty-guard so hook order is stable.
  const [idx, setIdx] = useState(0)
  const [flipped, setFlipped] = useState(false)
  const [known, setKnown] = useState<Record<number, boolean>>({})
  // null = natural order; a number seeds a deterministic shuffle (no Math.random,
  // so the order is stable across re-renders until the user reshuffles).
  const [shuffleSeed, setShuffleSeed] = useState<number | null>(null)
  const theme = useContext(DeckThemeContext)
  if (!total) return null

  const OK = '#10b981'
  const order = shuffleSeed == null ? cards.map((_, i) => i) : shuffleIndices(total, shuffleSeed)
  const clamp = (n: number) => Math.max(0, Math.min(total - 1, n))
  const cur = clamp(idx)
  const realIdx = order[cur]
  const card = cards[realIdx] || {}
  const hint = asStr(card.hint)
  const knownCount = Object.values(known).filter(Boolean).length
  const go = (n: number) => {
    setIdx(clamp(n))
    setFlipped(false)
  }

  return (
    <div className="flex flex-col gap-4">
      {title && <h3 className="text-lg font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>}
      <div className="flex items-center justify-between gap-3">
        <span
          className="inline-flex items-center rounded-full px-3 py-1 text-xs font-bold uppercase tracking-wide"
          style={{ background: theme.panel, color: theme.accent }}
        >
          Card {cur + 1} of {total}
        </span>
        <div className="flex items-center gap-4 text-xs" style={{ color: theme.muted }}>
          <span className="inline-flex items-center gap-1"><Check className="size-3.5" style={{ color: OK }} /> {knownCount} known</span>
          <button
            type="button"
            onClick={() => {
              setShuffleSeed((s) => (s == null ? 1 : s + 1))
              setIdx(0)
              setFlipped(false)
            }}
            className="inline-flex items-center gap-1 font-semibold transition-opacity hover:opacity-80"
            style={{ color: theme.accent }}
          >
            <Shuffle className="size-3.5" /> Shuffle
          </button>
        </div>
      </div>

      {/* Flip card — click to toggle front/back. 3D transforms are INLINE so they
          work regardless of the host's Tailwind build (the exported app too). */}
      <div style={{ perspective: '1400px' }}>
        <button
          type="button"
          onClick={() => setFlipped((f) => !f)}
          className="relative w-full"
          style={{ height: 280 }}
          aria-label="Flip card"
        >
          <div
            className="absolute inset-0"
            style={{
              transformStyle: 'preserve-3d',
              transition: 'transform 0.5s',
              transform: flipped ? 'rotateY(180deg)' : 'rotateY(0deg)',
            }}
          >
            {/* Front */}
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-3 rounded-2xl border p-8 text-center"
              style={{ background: theme.stage, borderColor: theme.panelBorder, color: theme.fg, backfaceVisibility: 'hidden', WebkitBackfaceVisibility: 'hidden' }}
            >
              <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: theme.muted }}>Question</span>
              <p className="text-xl font-semibold leading-snug" style={{ color: theme.fg }}>{asStr(card.front)}</p>
              {hint && <p className="text-sm" style={{ color: theme.muted }}>Hint: {hint}</p>}
              <span className="mt-2 inline-flex items-center gap-1 text-xs" style={{ color: theme.muted }}><RotateCw className="size-3.5" /> Click to flip</span>
            </div>
            {/* Back. OPAQUE fill (solid stage color + panel tint) so the answer
                text contrasts — `theme.panel` alone is semi-transparent on the
                built-in themes and composites to ~white over a light chat page,
                hiding the light foreground text. */}
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-3 rounded-2xl border p-8 text-center"
              style={{ backgroundColor: theme.bg, backgroundImage: `linear-gradient(0deg, ${theme.panel}, ${theme.panel})`, borderColor: theme.accent, color: theme.fg, backfaceVisibility: 'hidden', WebkitBackfaceVisibility: 'hidden', transform: 'rotateY(180deg)' }}
            >
              <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: theme.accent }}>Answer</span>
              <p className="text-lg leading-snug" style={{ color: theme.fg }}>{asStr(card.back)}</p>
            </div>
          </div>
        </button>
      </div>

      {/* Self-grade — marks the card and advances. */}
      <div className="flex items-center justify-center gap-2">
        <button
          type="button"
          onClick={() => { setKnown((k) => ({ ...k, [realIdx]: false })); go(cur + 1) }}
          className="rounded-xl border px-3 py-1.5 text-sm font-medium transition-opacity hover:opacity-80"
          style={{ borderColor: theme.panelBorder, color: theme.fg }}
        >
          Still learning
        </button>
        <button
          type="button"
          onClick={() => { setKnown((k) => ({ ...k, [realIdx]: true })); go(cur + 1) }}
          className="inline-flex items-center gap-1.5 rounded-xl px-3 py-1.5 text-sm font-semibold text-white transition-transform hover:scale-[1.02]"
          style={{ background: OK }}
        >
          <Check className="size-4" /> Got it
        </button>
      </div>

      {/* Nav: Prev / status dots (green = marked known) / Next */}
      <div className="flex items-center justify-between gap-3">
        <Button variant="outline" size="sm" className="gap-1" onClick={() => go(cur - 1)} disabled={cur === 0}>
          <ChevronLeft className="size-4" /> Prev
        </Button>
        <div className="flex flex-wrap items-center justify-center gap-1.5">
          {order.map((ri, i) => (
            <button
              key={i}
              aria-label={`Go to card ${i + 1}`}
              onClick={() => go(i)}
              className="h-2 rounded-full transition-all"
              style={
                i === cur
                  ? { width: 22, background: theme.accent }
                  : { width: 8, background: known[ri] ? OK : theme.muted, opacity: known[ri] ? 0.95 : 0.45 }
              }
            />
          ))}
        </div>
        <Button variant="outline" size="sm" className="gap-1" onClick={() => go(cur + 1)} disabled={cur >= total - 1}>
          Next <ChevronRight className="size-4" />
        </Button>
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

const LeafletMap = lazy(() => import('./LeafletMap'))

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

export function Unsupported({ node }: NodeProps) {
  return <div className="text-sm italic text-muted-foreground">Unsupported component: {asStr(node.component)}</div>
}

// Exported as `Card` for the registry; the local name avoids clashing with the
// shadcn Card primitive imported above.
export { Card_ as Card }
