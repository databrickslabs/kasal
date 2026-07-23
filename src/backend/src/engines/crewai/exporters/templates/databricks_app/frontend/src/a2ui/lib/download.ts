// Client-side download helpers for A2UI surfaces: CSV (tables), PowerPoint
// (presentations) and PNG snapshots (dashboards). Heavy libs are imported
// dynamically so they only load when a download is actually triggered.
import type { ComponentNode, Surface } from '../types'
import type { DeckTheme } from './deckThemes'
import { readableTextOn, seriesFromAccent } from './deckThemes'
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
// recharts Chart (chartType / data / xKey / yKeys). area/radar map to their
// native PowerPoint chart types; scatter falls back to a marker-less line (the
// data survives even though pptxgenjs scatter needs a bespoke format).
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
  const kind =
    type === 'pie' ? 'pie'
    : type === 'line' || type === 'scatter' ? 'line'
    : type === 'area' ? 'area'
    : type === 'radar' ? 'radar'
    : 'bar'
  return { kind, series }
}

// ---- Diagram → native PowerPoint shapes ------------------------------------
// Mirrors the renderer's archetype layouts with editable pptxgenjs shapes, so
// exported decks keep their diagrams instead of silently dropping them.
type PptxSlide = { addText: (...a: any[]) => unknown; addShape: (...a: any[]) => unknown }

interface DiagramExportItem {
  label: string
  detail?: string
  value?: string
  points: string[]
  children: DiagramExportItem[]
}

function normDiagramExportItems(v: unknown): DiagramExportItem[] {
  const arr = Array.isArray(v) ? v : []
  return arr
    .map((it): DiagramExportItem => {
      if (it && typeof it === 'object') {
        const o = it as Record<string, any>
        const points = (Array.isArray(o.points) ? o.points : Array.isArray(o.bullets) ? o.bullets : Array.isArray(o.items) ? o.items : [])
          .map((p: unknown) => String(p ?? '').trim())
          .filter(Boolean)
        return {
          label: String(o.label ?? o.title ?? o.name ?? o.step ?? o.text ?? '').trim(),
          detail: String(o.detail ?? o.description ?? o.subtitle ?? o.date ?? '').trim() || undefined,
          value: String(o.value ?? '').trim() || undefined,
          points,
          children: normDiagramExportItems(o.children),
        }
      }
      return { label: String(it ?? '').trim(), points: [], children: [] }
    })
    .filter((it) => it.label)
}

// PPTX wants "RRGGBB"; series colors come from seriesFromAccent (always hex).
const pptxHex = (c: string, fallback: string) => {
  const m = /^#?([0-9a-fA-F]{6})$/.exec((c || '').trim())
  return m ? m[1] : fallback
}

function addDiagramToSlide(
  slide: PptxSlide,
  node: ComponentNode,
  resolve: (v: unknown) => unknown,
  theme: { accent: string; bodyC: string; mutedC: string; panelBorderC: string },
  area: { x: number; y: number; w: number; h: number },
) {
  const raw = String(node.archetype ?? 'process').toLowerCase().replace(/[\s_-]/g, '')
  const archetype =
    raw === 'matrix' || raw === 'quadrant' ? 'matrix2x2'
    : raw === 'org' || raw === 'orgchart' || raw === 'tree' ? 'hierarchy'
    : raw === 'versus' || raw === 'vs' ? 'comparison'
    : raw === 'loop' ? 'cycle'
    : raw === 'flow' || raw === 'flowchart' || raw === 'steps' ? 'process'
    : raw === 'milestones' || raw === 'roadmap' ? 'timeline'
    : raw === 'pipeline' ? 'funnel'
    : raw
  const items = normDiagramExportItems(resolve(node.items))
  if (!items.length) return
  const colors = seriesFromAccent(theme.accent, Math.max(items.length, 2))
  const colorAt = (i: number) => pptxHex(colors[i % colors.length], '2563EB')
  const onColor = (i: number) => pptxHex(readableTextOn(colors[i % colors.length]), 'FFFFFF')
  const { x, y, w, h } = area
  const n = items.length

  if (archetype === 'timeline') {
    const midY = y + h * 0.45
    const colW = w / n
    slide.addShape('line', { x, y: midY, w, h: 0, line: { color: theme.panelBorderC, width: 1.5 } })
    items.forEach((it, i) => {
      const cx = x + colW * (i + 0.5)
      slide.addShape('ellipse', { x: cx - 0.09, y: midY - 0.09, w: 0.18, h: 0.18, fill: { color: colorAt(i) } })
      slide.addText(it.label, { x: cx - colW / 2 + 0.05, y: midY + 0.2, w: colW - 0.1, h: 0.4, fontSize: 13, bold: true, color: theme.bodyC, align: 'center' })
      if (it.detail) slide.addText(it.detail, { x: cx - colW / 2 + 0.05, y: midY + 0.62, w: colW - 0.1, h: 0.6, fontSize: 10, color: theme.mutedC, align: 'center', valign: 'top' })
    })
    return
  }

  if (archetype === 'funnel' || archetype === 'pyramid') {
    const rowH = Math.min(0.7, (h - 0.1 * (n - 1)) / n)
    items.forEach((it, i) => {
      const pct = archetype === 'funnel' ? 1 - (i * 0.55) / Math.max(n - 1, 1) : 0.42 + (i * 0.58) / Math.max(n - 1, 1)
      const rw = w * pct
      const rx = x + (w - rw) / 2
      const ry = y + i * (rowH + 0.1)
      slide.addShape('roundRect', { x: rx, y: ry, w: rw, h: rowH, fill: { color: colorAt(i) }, rectRadius: 0.05 })
      const label = it.value ? `${it.label} — ${it.value}` : it.label
      slide.addText(label, { x: rx, y: ry, w: rw, h: rowH, fontSize: 13, bold: true, color: onColor(i), align: 'center', valign: 'middle' })
    })
    return
  }

  if (archetype === 'comparison') {
    const cols = items.slice(0, 3)
    const gap = 0.4
    const colW = (w - gap * (cols.length - 1)) / cols.length
    cols.forEach((it, i) => {
      const cx = x + i * (colW + gap)
      slide.addShape('roundRect', { x: cx, y, w: colW, h: 0.55, fill: { color: colorAt(i) }, rectRadius: 0.04 })
      slide.addText(it.label, { x: cx, y, w: colW, h: 0.55, fontSize: 14, bold: true, color: onColor(i), align: 'center', valign: 'middle' })
      slide.addShape('roundRect', { x: cx, y: y + 0.65, w: colW, h: Math.max(h - 0.75, 0.6), fill: { color: 'FFFFFF', transparency: 100 }, line: { color: theme.panelBorderC, width: 1 }, rectRadius: 0.04 })
      if (it.points.length) {
        slide.addText(
          it.points.map((p) => ({ text: p, options: { bullet: { code: '2022', indent: 12 }, breakLine: true, paraSpaceAfter: 6 } })),
          { x: cx + 0.15, y: y + 0.8, w: colW - 0.3, h: Math.max(h - 1.0, 0.5), fontSize: 12, color: theme.bodyC, valign: 'top' },
        )
      }
    })
    return
  }

  if (archetype === 'matrix2x2') {
    const quads = items.slice(0, 4)
    const gap = 0.15
    const qw = (w - gap) / 2
    const qh = (h - gap - 0.4) / 2
    quads.forEach((it, i) => {
      const qx = x + (i % 2) * (qw + gap)
      const qy = y + Math.floor(i / 2) * (qh + gap)
      slide.addShape('roundRect', { x: qx, y: qy, w: qw, h: qh, fill: { color: 'FFFFFF', transparency: 100 }, line: { color: colorAt(i), width: 1.75 }, rectRadius: 0.04 })
      slide.addText(
        [
          { text: it.label, options: { bold: true, fontSize: 13, breakLine: true } },
          ...(it.detail ? [{ text: it.detail, options: { fontSize: 10.5, color: theme.mutedC } }] : []),
        ],
        { x: qx + 0.12, y: qy + 0.1, w: qw - 0.24, h: qh - 0.2, color: theme.bodyC, valign: 'top' },
      )
    })
    const xLabel = String(node.xLabel ?? '').trim()
    if (xLabel) slide.addText(`${xLabel} →`, { x, y: y + h - 0.35, w, h: 0.3, fontSize: 10, bold: true, color: theme.mutedC, align: 'center' })
    const yLabel = String(node.yLabel ?? '').trim()
    if (yLabel) slide.addText(`${yLabel} →`, { x: x - 0.4, y: y + h / 2 - 0.15, w: 1.4, h: 0.3, fontSize: 10, bold: true, color: theme.mutedC, align: 'center', rotate: 270 })
    return
  }

  if (archetype === 'hierarchy') {
    const root = items[0]
    const children = root.children.length ? root.children : items.slice(1)
    const rw = Math.min(3.2, w * 0.4)
    const rx = x + (w - rw) / 2
    slide.addShape('roundRect', { x: rx, y, w: rw, h: 0.55, fill: { color: pptxHex(theme.accent, '2563EB') }, rectRadius: 0.05 })
    slide.addText(root.label, { x: rx, y, w: rw, h: 0.55, fontSize: 14, bold: true, color: pptxHex(readableTextOn(theme.accent), 'FFFFFF'), align: 'center', valign: 'middle' })
    if (children.length) {
      const gap = 0.25
      const cw = (w - gap * (children.length - 1)) / children.length
      const cy = y + 1.1
      children.forEach((ch, i) => {
        const cx = x + i * (cw + gap)
        slide.addShape('line', { x: cx + cw / 2, y: y + 0.55, w: 0, h: cy - y - 0.55, line: { color: theme.panelBorderC, width: 1 } })
        slide.addShape('roundRect', { x: cx, y: cy, w: cw, h: 0.5, fill: { color: 'FFFFFF', transparency: 100 }, line: { color: colorAt(i), width: 1.5 }, rectRadius: 0.04 })
        slide.addText(ch.label, { x: cx, y: cy, w: cw, h: 0.5, fontSize: 12, bold: true, color: theme.bodyC, align: 'center', valign: 'middle' })
        if (ch.children.length) {
          slide.addText(
            ch.children.map((g) => ({ text: g.label, options: { bullet: { code: '2022', indent: 10 }, breakLine: true, paraSpaceAfter: 4 } })),
            { x: cx + 0.08, y: cy + 0.6, w: cw - 0.16, h: Math.max(y + h - (cy + 0.65), 0.4), fontSize: 10.5, color: theme.mutedC, valign: 'top' },
          )
        }
      })
    }
    return
  }

  if (archetype === 'cycle') {
    const cx = x + w / 2
    const cy = y + h / 2
    const rx = w * 0.38
    const ry = h * 0.36
    const bw = 1.9
    const bh = 0.5
    items.forEach((it, i) => {
      const a = (2 * Math.PI * i) / n - Math.PI / 2
      const px = cx + rx * Math.cos(a) - bw / 2
      const py = cy + ry * Math.sin(a) - bh / 2
      slide.addShape('roundRect', { x: px, y: py, w: bw, h: bh, fill: { color: 'FFFFFF', transparency: 100 }, line: { color: colorAt(i), width: 1.5 }, rectRadius: 0.25 })
      slide.addText(`${i + 1}. ${it.label}`, { x: px, y: py, w: bw, h: bh, fontSize: 11.5, bold: true, color: theme.bodyC, align: 'center', valign: 'middle' })
    })
    slide.addText('⟳', { x: cx - 0.4, y: cy - 0.4, w: 0.8, h: 0.8, fontSize: 40, color: theme.mutedC, align: 'center', valign: 'middle' })
    return
  }

  // process (default): a chevron ribbon with the label inside each step.
  const gap = 0.06
  const stepW = (w - gap * (n - 1)) / n
  const stepH = Math.min(1.1, h * 0.55)
  const sy = y + (h - stepH) / 2 - 0.2
  items.forEach((it, i) => {
    const sx = x + i * (stepW + gap)
    slide.addShape('chevron', { x: sx, y: sy, w: stepW, h: stepH, fill: { color: colorAt(i) } })
    slide.addText(
      [
        { text: `STEP ${i + 1}`, options: { fontSize: 8.5, bold: true, breakLine: true } },
        { text: it.label, options: { fontSize: 12, bold: true } },
      ],
      { x: sx + 0.08, y: sy, w: stepW - 0.16, h: stepH, color: onColor(i), align: 'center', valign: 'middle' },
    )
    if (it.detail) {
      slide.addText(it.detail, { x: sx, y: sy + stepH + 0.08, w: stepW, h: 0.55, fontSize: 9.5, color: theme.mutedC, align: 'center', valign: 'top' })
    }
  })
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

    // A chart / diagram / table slide renders the actual visual (PowerPoint-
    // native), not blank — the previous text-only export dropped them entirely.
    const chartNode = findNode(sid, byId, (n) => n.component === 'Chart')
    const tableNode = findNode(sid, byId, (n) => n.component === 'Table')
    const diagramNode = findNode(sid, byId, (n) => n.component === 'Diagram')
    const panelBorderC = dark ? '333C45' : 'E2E6EA'

    const addChartAt = (cx: number, cw: number) => {
      const { kind, series } = chartSpec(chartNode as ComponentNode, resolve)
      const data = kind === 'pie'
        ? [{ name: series[0]?.name || 'series', labels: series[0]?.labels || [], values: series[0]?.values || [] }]
        : series
      const chartType = pptx.ChartType[kind as 'bar' | 'line' | 'pie' | 'area' | 'radar']
      slide.addChart(chartType, data, {
        x: cx, y: areaY, w: cw, h: areaH - 0.1,
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
    }
    const addDiagramAt = (dx: number, dw: number) => {
      addDiagramToSlide(
        slide,
        diagramNode as ComponentNode,
        resolve,
        { accent: theme?.accent || '#2563EB', bodyC, mutedC, panelBorderC },
        { x: dx, y: areaY + 0.1, w: dw, h: areaH - 0.3 },
      )
    }

    // Text lines from the slide body (visual components are not text-extracted).
    const out: string[] = []
    ;(node?.children || []).forEach((c) => collectText(c, byId, resolve, out))
    // Graph / Sequence have no native-shape export — extract their content as
    // text lines so those slides don't export blank.
    const graphNode = findNode(sid, byId, (n) => n.component === 'Graph')
    if (graphNode) {
      const edges = resolve(graphNode.edges)
      if (Array.isArray(edges)) {
        edges.forEach((e) => {
          const o = (e ?? {}) as Record<string, unknown>
          const from = String(o.from ?? o.source ?? '')
          const to = String(o.to ?? o.target ?? '')
          if (from && to) out.push(`• ${from} → ${to}${o.label ? ` (${String(o.label)})` : ''}`)
        })
      }
    }
    const seqNode = findNode(sid, byId, (n) => n.component === 'Sequence')
    if (seqNode) {
      const msgs = resolve(seqNode.messages)
      if (Array.isArray(msgs)) {
        msgs.forEach((m) => {
          const o = (m ?? {}) as Record<string, unknown>
          const from = String(o.from ?? o.source ?? '')
          const to = String(o.to ?? o.target ?? '')
          if (from && to) out.push(`• ${from} → ${to}${o.text ? `: ${String(o.text)}` : ''}`)
        })
      }
    }
    const lines = deMarkdown(out.join('\n')).split('\n').filter(Boolean)

    // Two-column: text on the left half, the visual on the right half — mirrors
    // the renderer's variant='two-column' layout.
    const primaryVisual = chartNode ? 'chart' : diagramNode ? 'diagram' : null
    const twoCol = variant.replace(/[_\s]/g, '-') === 'two-column' && primaryVisual && lines.length > 0

    if (twoCol) {
      const paras = lines.map((t) => ({
        text: t.replace(/^•\s*/, ''),
        options: { breakLine: true, paraSpaceAfter: 10, bullet: /^•\s*/.test(t) ? { code: '2022', indent: 16 } : false },
      }))
      slide.addText(paras, {
        x: 0.7, y: areaY, w: 5.6, h: areaH,
        fontSize: 16, color: bodyC, valign: 'middle', align: 'left', lineSpacingMultiple: 1.25,
      })
      if (primaryVisual === 'chart') addChartAt(6.6, 6.0)
      else addDiagramAt(6.6, 6.0)
      continue
    }

    if (chartNode) {
      addChartAt(0.7, 11.9)
      continue
    }

    if (diagramNode) {
      addDiagramAt(0.9, 11.5)
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
        border: { type: 'solid', color: panelBorderC, pt: 1 },
        autoPage: false,
      })
      continue
    }

    // Text body — vertically CENTERED (matches the renderer's justify-center) with
    // per-paragraph spacing; disc bullets where the source used a list, numbered
    // rows for variant='agenda' (mirrors the renderer's numbered agenda layout).
    if (lines.length) {
      const agenda = variant === 'agenda'
      const paras = lines.map((t) => {
        const isBullet = /^•\s*/.test(t)
        return {
          text: t.replace(/^•\s*/, ''),
          options: {
            breakLine: true,
            paraSpaceAfter: 12,
            bullet: agenda ? { type: 'number' as const, indent: 18 } : isBullet ? { code: '2022', indent: 18 } : false,
          },
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
