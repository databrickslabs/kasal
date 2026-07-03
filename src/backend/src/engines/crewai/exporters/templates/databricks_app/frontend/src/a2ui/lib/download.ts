// Client-side download helpers for A2UI surfaces: CSV (tables), PowerPoint
// (presentations) and PNG snapshots (dashboards). Heavy libs are imported
// dynamically so they only load when a download is actually triggered.
import type { ComponentNode, Surface } from '../types'
import type { DeckTheme } from './deckThemes'
import { resolveValue } from '../resolve'

function triggerDownload(href: string, filename: string) {
  const a = document.createElement('a')
  a.href = href
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob)
  triggerDownload(url, filename)
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

// ---- CSV (Table) ---------------------------------------------------------
export function tableToCsv(columns: string[], rows: unknown[][]): string {
  const esc = (v: unknown) => {
    const s = v == null ? '' : String(v)
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
  }
  const lines: string[] = []
  if (columns.length) lines.push(columns.map(esc).join(','))
  for (const row of rows) lines.push((Array.isArray(row) ? row : [row]).map(esc).join(','))
  return lines.join('\n')
}

export function downloadCsv(columns: string[], rows: unknown[][], filename = 'table.csv') {
  downloadBlob(new Blob([tableToCsv(columns, rows)], { type: 'text/csv;charset=utf-8' }), filename)
}

// ---- PNG (Dashboard / any surface element) -------------------------------
export async function downloadElementPng(el: HTMLElement, filename = 'dashboard.png') {
  const { toPng } = await import('html-to-image')
  const dataUrl = await toPng(el, { backgroundColor: '#ffffff', pixelRatio: 2 })
  triggerDownload(dataUrl, filename)
}

// ---- PPTX (Presentation) -------------------------------------------------
function collectText(
  id: string,
  byId: Record<string, ComponentNode>,
  resolve: (v: unknown) => unknown,
  out: string[],
) {
  const node = byId[id]
  if (!node) return
  const push = (v: unknown) => {
    const s = String(resolve(v) ?? '').trim()
    if (s) out.push(s)
  }
  switch (node.component) {
    case 'Heading':
    case 'Text':
      push(node.text)
      break
    case 'Markdown':
      push(node.content)
      break
    case 'KeyValue':
      out.push(`${String(resolve(node.label) ?? '')}: ${String(resolve(node.value) ?? '')}`.trim())
      break
    case 'List': {
      const items = resolve(node.items)
      if (Array.isArray(items)) items.forEach((it) => out.push(`• ${String(it)}`))
      break
    }
  }
  ;(node.children || []).forEach((c) => collectText(c, byId, resolve, out))
}

// Strip light markdown so it reads cleanly as plain slide text.
const deMarkdown = (s: string) =>
  s
    .split('\n')
    .map((l) => l.replace(/^#+\s*/, '').replace(/^[-*]\s+/, '• ').replace(/[*_`]/g, '').trim())
    .filter(Boolean)
    .join('\n')

// A 6-hex color for pptxgenjs (which wants "RRGGBB", no '#'). rgba()/named
// colors aren't supported there, so fall back when the theme value isn't hex.
function hex(c: string | undefined, fallback: string): string {
  const m = c && /^#?([0-9a-fA-F]{6})$/.exec(c.trim())
  return m ? m[1] : fallback
}

// Mirrors CHART_COLORS in components.tsx (PPTX wants "RRGGBB").
const CHART_HEX = ['2563EB', '10B981', 'F59E0B', 'EF4444', '06B6D4', 'A855F7']

// First descendant node (incl. self) matching the predicate, depth-first.
function findNode(
  id: string,
  byId: Record<string, ComponentNode>,
  pred: (n: ComponentNode) => boolean,
  depth = 0,
): ComponentNode | null {
  if (depth > 6) return null
  const n = byId[id]
  if (!n) return null
  if (pred(n)) return n
  for (const c of Array.isArray(n.children) ? n.children : []) {
    const f = findNode(c, byId, pred, depth + 1)
    if (f) return f
  }
  return null
}

// A Chart node → pptxgenjs chart spec (type + series), mirroring the on-screen
// recharts Chart (chartType / data / xKey / yKeys).
function chartSpec(node: ComponentNode, resolve: (v: unknown) => unknown) {
  const type = String(node.chartType ?? 'bar').toLowerCase()
  const rows = (() => {
    const d = resolve(node.data)
    return Array.isArray(d) ? (d as Record<string, unknown>[]) : []
  })()
  const xKey = String(node.xKey ?? 'name')
  const yKeys = Array.isArray(node.yKeys) ? node.yKeys.map(String) : []
  const keys = yKeys.length ? yKeys : ['value']
  const labels = rows.map((r) => String(r?.[xKey] ?? ''))
  const series = keys.map((k) => ({ name: k, labels, values: rows.map((r) => Number(r?.[k]) || 0) }))
  const kind = type === 'pie' ? 'pie' : type === 'line' ? 'line' : 'bar'
  return { kind, series }
}

export async function downloadPptx(
  surface: Surface,
  theme?: DeckTheme,
  filename = 'presentation.pptx',
) {
  const PptxGenJS = (await import('pptxgenjs')).default
  const pptx = new PptxGenJS()
  pptx.layout = 'LAYOUT_WIDE' // 13.33 x 7.5 in

  const dark = !!theme?.dark
  const bg = hex(theme?.bg, 'FFFFFF')
  const titleC = hex(theme?.title, dark ? 'FFFFFF' : '111827')
  const bodyC = hex(theme?.fg, dark ? 'E5E7EB' : '333333')
  const kickerC = hex(theme?.kicker, '2563EB')
  const accentC = hex(theme?.accent, '2563EB')
  const mutedC = hex(theme?.muted, dark ? '9AA4B2' : '6B7280')

  const byId = Object.fromEntries((surface.components || []).map((c) => [c.id, c]))
  const resolve = (v: unknown) => resolveValue(v, surface.dataModel ?? {})
  const root = byId[surface.root]
  const slideIds = root?.component === 'SlideDeck' ? root.children || [] : [surface.root]

  for (const sid of slideIds) {
    const node = byId[sid]
    const slide = pptx.addSlide()
    slide.background = { color: bg }
    const variant = String(node?.variant ?? '').toLowerCase()
    const kicker = String(resolve(node?.kicker) ?? '').trim()
    const title = String(resolve(node?.title) ?? '').trim()

    // Centered title / section divider (mirrors the renderer's centered layout).
    if (variant === 'title' || variant === 'section') {
      if (kicker) slide.addText(kicker.toUpperCase(), { x: 0.6, y: 2.0, w: 12.1, h: 0.4, fontSize: 14, bold: true, color: kickerC, align: 'center', charSpacing: 3 })
      if (title) slide.addText(title, { x: 0.8, y: 2.5, w: 11.7, h: 1.8, fontSize: 44, bold: true, color: titleC, align: 'center' })
      slide.addShape('rect', { x: 5.92, y: 4.55, w: 1.5, h: 0.06, fill: { color: accentC } })
      const sub = String(resolve(node?.subtitle) ?? '').trim()
      if (sub) slide.addText(sub, { x: 1.5, y: 4.85, w: 10.3, h: 1.2, fontSize: 20, color: mutedC, align: 'center' })
      continue
    }

    // Header band: kicker → title → accent rule (top-left), shared by all
    // remaining variants.
    let y = 0.5
    if (kicker) {
      slide.addText(kicker.toUpperCase(), { x: 0.6, y, w: 12.1, h: 0.35, fontSize: 12, bold: true, color: kickerC, charSpacing: 3 })
      y += 0.45
    }
    if (title) {
      slide.addText(title, { x: 0.6, y, w: 12.1, h: 0.95, fontSize: 32, bold: true, color: titleC })
      y += 1.0
    }
    slide.addShape('rect', { x: 0.62, y: y - 0.1, w: 0.9, h: 0.06, fill: { color: accentC } })
    y += 0.25

    if (variant === 'stats') {
      const kvs = (node?.children || []).map((id) => byId[id]).filter((n) => n && n.component === 'KeyValue')
      const n = Math.max(kvs.length, 1)
      const tileW = 12.1 / n
      kvs.forEach((kv, i) => {
        const vx = 0.6 + i * tileW
        slide.addText(String(resolve(kv.value) ?? ''), { x: vx, y: 3.0, w: tileW - 0.25, h: 1.0, fontSize: 44, bold: true, color: accentC })
        slide.addText(String(resolve(kv.label) ?? ''), { x: vx, y: 4.15, w: tileW - 0.25, h: 0.8, fontSize: 15, color: bodyC })
      })
      continue
    }

    // Optional subtitle lead-in under the title (matches the renderer).
    const sub = String(resolve(node?.subtitle) ?? '').trim()
    if (sub) {
      slide.addText(sub, { x: 0.6, y, w: 12.1, h: 0.7, fontSize: 18, color: mutedC, valign: 'top', lineSpacingMultiple: 1.1 })
      y += 0.75
    }

    const areaY = y + 0.05
    const areaH = Math.max(7.2 - areaY, 1)

    // A chart / table slide renders the actual visual (PowerPoint-native), not
    // blank — the previous text-only export dropped them entirely.
    const chartNode = findNode(sid, byId, (n) => n.component === 'Chart')
    const tableNode = findNode(sid, byId, (n) => n.component === 'Table')

    if (chartNode) {
      const { kind, series } = chartSpec(chartNode, resolve)
      const data = kind === 'pie'
        ? [{ name: series[0]?.name || 'series', labels: series[0]?.labels || [], values: series[0]?.values || [] }]
        : series
      const chartType = pptx.ChartType[kind as 'bar' | 'line' | 'pie']
      slide.addChart(chartType, data, {
        x: 0.7, y: areaY, w: 11.9, h: areaH - 0.1,
        chartColors: CHART_HEX,
        showLegend: kind !== 'pie' ? series.length > 1 : true,
        legendPos: 'b',
        legendColor: bodyC,
        showTitle: false,
        catAxisLabelColor: bodyC,
        valAxisLabelColor: bodyC,
        showValue: kind === 'pie',
        dataLabelColor: dark ? 'FFFFFF' : '333333',
      })
      continue
    }

    if (tableNode) {
      const cols = (Array.isArray(tableNode.columns) ? tableNode.columns : []).map((c) => String(c))
      const rawRows = resolve(tableNode.rows)
      const rows = Array.isArray(rawRows) ? rawRows : []
      const head = cols.map((c) => ({ text: c, options: { bold: true, color: 'FFFFFF', fill: { color: titleC } } }))
      const bodyRows = rows.map((r) =>
        (Array.isArray(r) ? r : []).map((cell) => ({ text: String(cell ?? ''), options: { color: bodyC } })),
      )
      slide.addTable(head.length ? [head, ...bodyRows] : bodyRows, {
        x: 0.6, y: areaY, w: 12.1,
        fontSize: 13, color: bodyC, valign: 'middle',
        border: { type: 'solid', color: dark ? '333C45' : 'E2E6EA', pt: 1 },
        autoPage: false,
      })
      continue
    }

    // Text body — vertically CENTERED (matches the renderer's justify-center) with
    // per-paragraph spacing, and disc bullets only where the source used a list.
    const out: string[] = []
    ;(node?.children || []).forEach((c) => collectText(c, byId, resolve, out))
    const lines = deMarkdown(out.join('\n')).split('\n').filter(Boolean)
    if (lines.length) {
      const paras = lines.map((t) => {
        const isBullet = /^•\s*/.test(t)
        return {
          text: t.replace(/^•\s*/, ''),
          options: { breakLine: true, paraSpaceAfter: 12, bullet: isBullet ? { code: '2022', indent: 18 } : false },
        }
      })
      slide.addText(paras, {
        x: 0.7, y: areaY, w: 11.9, h: areaH,
        fontSize: 20, color: bodyC, valign: 'middle', align: 'left', lineSpacingMultiple: 1.3,
      })
    }
  }
  await pptx.writeFile({ fileName: filename })
}
