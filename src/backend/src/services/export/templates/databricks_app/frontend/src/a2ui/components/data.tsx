/**
 * Data renderers: Table, Chart (recharts) and Forecast.
 */
import { Fragment, useCallback, useContext, useMemo, useState } from 'react'
import { ResponsiveContainer, BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, ComposedChart, Area, AreaChart, ScatterChart, Scatter, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts'
import type { ComponentNode, NodeProps } from '../types'
import { Download } from 'lucide-react'
import { Button } from '../ui/button'
import { downloadCsv } from '../lib/download'
import { DeckThemeContext, seriesFromAccent } from '../lib/deckThemes'
import { SurfaceChromeContext } from '../lib/surfaceContext'
import { cn } from '../lib/utils'
import { asArr, asNum, asStr, numericValue } from './values'

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
        ) : type === 'area' ? (
          <AreaChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
            {keys.map((k, i) => (
              <Area key={k} type="monotone" dataKey={k} stroke={series[i % series.length]} fill={series[i % series.length]} fillOpacity={0.25} />
            ))}
          </AreaChart>
        ) : type === 'scatter' ? (
          // Each series becomes {x, y} pairs so multiple yKeys plot as separate
          // clouds; a numeric x column gets a true numeric axis (correlations),
          // a categorical one falls back to a category axis.
          <ScatterChart margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis
              dataKey="x"
              name={xKey}
              type={data.every((r) => asNum((r as Record<string, unknown>)?.[xKey]) != null) ? 'number' : 'category'}
            />
            <YAxis dataKey="y" type="number" />
            <Tooltip cursor={{ strokeDasharray: '3 3' }} /><Legend />
            {keys.map((k, i) => (
              <Scatter
                key={k}
                name={k}
                data={data.map((r) => ({ x: (r as Record<string, unknown>)?.[xKey], y: asNum((r as Record<string, unknown>)?.[k]) }))}
                fill={series[i % series.length]}
              />
            ))}
          </ScatterChart>
        ) : type === 'radar' ? (
          <RadarChart data={data} cx="50%" cy="50%" outerRadius="70%">
            <PolarGrid stroke="var(--color-border)" />
            <PolarAngleAxis dataKey={xKey} />
            <PolarRadiusAxis />
            <Tooltip /><Legend />
            {keys.map((k, i) => (
              <Radar key={k} name={k} dataKey={k} stroke={series[i % series.length]} fill={series[i % series.length]} fillOpacity={0.25} />
            ))}
          </RadarChart>
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
