// Client-side download helpers for A2UI surfaces: CSV (tables), PowerPoint
// (presentations) and PNG snapshots (dashboards). Heavy libs are imported
// dynamically so they only load when a download is actually triggered.
import type { ComponentNode, Surface } from '../types'
import type { DeckTheme } from './deckThemes'
import { readableTextOn, seriesFromAccent } from './deckThemes'
import { resolveValue } from '../resolve'
import { normSlideSources } from './slideSources'
import { addDiagramToSlide, pptxHex } from './pptxDiagram'

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

// PowerPoint's own "shrink text on overflow", the exported counterpart to the
// renderer's `FitBox`. A slide box here has a FIXED height in inches, so a body
// that needs more lines than fit would otherwise spill over the next element (or
// the sources footer) in the opened .pptx.
//
// `fit` ONLY — do NOT also pass the deprecated `shrinkText: true`. Both emit
// `<a:normAutofit/>`, and two of them inside one `<a:bodyPr>` violates the schema:
// PowerPoint then refuses the whole file with "found a problem with content …
// attempt to repair", which is indistinguishable from a corrupt download.
const SHRINK_TO_FIT = { fit: 'shrink' as const }

// …and `fit` alone is NOT enough for a body that is already too long. A bare
// `<a:normAutofit/>` records the INTENT to shrink but carries no fontScale, and
// PowerPoint only computes one when the text is next edited — so on first open the
// box renders at full size and, with `valign: 'middle'`, overflows BOTH ends: over
// the title above and the sources footer below. That is the "text on top of the
// title" in the exported deck.
//
// So size the text here, from the geometry we already know. Deliberately crude —
// average character width and line height as fractions of the point size — because
// the alternative is measuring text in a DOM this export path does not have. It
// only ever shrinks, so a slide that already fits is untouched.
//
// The constants are tuned to OVER-estimate. Under-estimating puts text through the
// sources footer, which is the visible bug; over-estimating just leaves a slide
// slightly roomier than it had to be. An earlier pass used 0.5/1.42 and forgot
// paragraph spacing entirely, so it chose 19pt where 16pt was the real limit and
// the body still ran off the box.
const AVG_CHAR_W = 0.55  // mean glyph advance ÷ font size, for a humanist sans
const LINE_H = 1.3       // the `lineSpacingMultiple` used on these bodies
const LINE_PAD = 1.2     // ascent/descent slack the line box adds on top of that
export function fitFontSize(
  lines: string[],
  boxW: number,
  boxH: number,
  desired: number,
  min = 8,
  /** Per-paragraph spacing in POINTS — `paraSpaceAfter`, which also costs height. */
  paraSpace = 12,
): number {
  const heightAt = (size: number) => {
    const perLine = Math.max(Math.floor((boxW * 72) / (size * AVG_CHAR_W)), 8)
    const wrapped = lines.reduce(
      (total, t) => total + Math.max(Math.ceil(t.length / perLine), 1),
      0,
    )
    return (wrapped * size * LINE_H * LINE_PAD + lines.length * paraSpace) / 72
  }
  for (let size = desired; size > min; size -= 0.5) {
    if (heightAt(size) <= boxH) return size
  }
  return min
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
  // Panel fill for the 'boxes' grid, matching the renderer's themed panels. The
  // theme's `panel` is often an rgba()/gradient the PPTX color parser cannot take,
  // so `hex` falls back to a flat tone that reads correctly on either background.
  const panelC = hex(theme?.panel, dark ? '1B2430' : 'F3F5F7')
  const panelBorderC = dark ? '333C45' : 'E2E6EA'

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

    // Notes and citations are attached BEFORE the per-variant `continue`s below,
    // so no layout can silently drop them from the download.
    // Speaker notes become real PowerPoint notes (presenter view), not slide text.
    const notes = String(resolve(node?.notes) ?? '').trim()
    if (notes) slide.addNotes(notes)
    // Citations as a footer band, mirroring the on-screen sources footer.
    const srcs = normSlideSources(resolve(node?.sources))
    // Body layouts below must stop above this band; see `areaBottom`.
    const hasSources = srcs.length > 0
    if (srcs.length) {
      slide.addText(
        `Sources — ${srcs.map((s, i) => `${i + 1}. ${s.label}`).join('    ')}`,
        { x: 0.6, y: 6.75, w: 12.1, h: 0.45, fontSize: 10, color: mutedC, valign: 'top' },
      )
    }

    // Centered title / section divider (mirrors the renderer's centered layout).
    if (variant === 'title' || variant === 'section') {
      if (kicker) slide.addText(kicker.toUpperCase(), { x: 0.6, y: 2.0, w: 12.1, h: 0.4, fontSize: 14, bold: true, color: kickerC, align: 'center', charSpacing: 3 })
      if (title) slide.addText(title, { x: 0.8, y: 2.5, w: 11.7, h: 1.8, fontSize: 44, bold: true, color: titleC, align: 'center', ...SHRINK_TO_FIT })
      slide.addShape('rect', { x: 5.92, y: 4.55, w: 1.5, h: 0.06, fill: { color: accentC } })
      const sub = String(resolve(node?.subtitle) ?? '').trim()
      if (sub) slide.addText(sub, { x: 1.5, y: 4.85, w: 10.3, h: 1.2, fontSize: 20, color: mutedC, align: 'center', ...SHRINK_TO_FIT })
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
      slide.addText(title, { x: 0.6, y, w: 12.1, h: 0.95, fontSize: 32, bold: true, color: titleC, ...SHRINK_TO_FIT })
      y += 1.0
    }
    slide.addShape('rect', { x: 0.62, y: y - 0.1, w: 0.9, h: 0.06, fill: { color: accentC } })
    y += 0.25

    // 'kpi-split' carries a KeyValue band ABOVE a body. Draw the band here and
    // let the generic body handling below place the rest, so the tiles survive
    // the export instead of being flattened into "label: value" bullet lines.
    if (variant === 'kpi-split') {
      const kvs = (node?.children || []).map((id) => byId[id]).filter((n) => n && n.component === 'KeyValue')
      if (kvs.length) {
        // Panels with the label pinned to the BOTTOM, mirroring the renderer. The
        // old version was bare text at a fixed 26pt, so a long value
        // ("0.168 toe per 000 USD (PPP, 2023)") wrapped over its neighbour and
        // dragged its label down out of line with the rest of the row.
        const gap = 0.18
        const bandH = 1.15
        const tileW = (12.1 - gap * (kvs.length - 1)) / kvs.length
        kvs.forEach((kv, i) => {
          const vx = 0.6 + i * (tileW + gap)
          const value = String(resolve(kv.value) ?? '')
          slide.addShape('roundRect', {
            x: vx, y, w: tileW, h: bandH,
            fill: { color: panelC }, line: { color: panelBorderC, width: 1 }, rectRadius: 0.06,
          })
          slide.addText(value, {
            x: vx + 0.14, y: y + 0.1, w: tileW - 0.28, h: bandH - 0.48,
            fontSize: fitFontSize([value], tileW - 0.28, bandH - 0.48, 22, 11, 0),
            bold: true, color: accentC, valign: 'middle', ...SHRINK_TO_FIT,
          })
          slide.addText(String(resolve(kv.label) ?? ''), {
            x: vx + 0.14, y: y + bandH - 0.4, w: tileW - 0.28, h: 0.32,
            fontSize: 10, color: bodyC, valign: 'middle', ...SHRINK_TO_FIT,
          })
        })
        y += bandH + 0.25
      }
    }

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
      slide.addText(sub, { x: 0.6, y, w: 12.1, h: 0.7, fontSize: 18, color: mutedC, valign: 'top', lineSpacingMultiple: 1.1, ...SHRINK_TO_FIT })
      y += 0.75
    }

    const areaY = y + 0.05
    // Stop ABOVE the sources footer (drawn at y = 6.75) when there is one, instead
    // of always running to 7.2 and printing the body straight through it.
    const areaBottom = hasSources ? 6.65 : 7.2
    const areaH = Math.max(areaBottom - areaY, 1)

    // A chart / diagram / table slide renders the actual visual (PowerPoint-
    // native), not blank — the previous text-only export dropped them entirely.
    const chartNode = findNode(sid, byId, (n) => n.component === 'Chart')
    const tableNode = findNode(sid, byId, (n) => n.component === 'Table')
    const diagramNode = findNode(sid, byId, (n) => n.component === 'Diagram')

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

    // 'agenda' is the CONTENTS page: a number tile, a bold section title and an
    // italic descriptor, flowed into `columns` columns column-first. Mirrors the
    // renderer exactly, so the downloaded deck matches what Kasal shows.
    if (variant === 'agenda') {
      const kids = node?.children || []
      const rowsText = kids.map((cid) => {
        const acc: string[] = []
        collectText(cid, byId, resolve, acc)
        const flat = deMarkdown(acc.join('\n')).split('\n').filter(Boolean).join(' ')
        // Authors write "Name — descriptor" (and sometimes a leading "01 — ", which
        // the tile already shows). Split on the em-dash, drop the number.
        const parts = flat.replace(/^•\s*/, '').split(/\s+[—–]\s+/)
        if (/^\d{1,2}$/.test(parts[0] || '')) parts.shift()
        return { title: parts.shift() || '', desc: parts.join(' · ') }
      }).filter((r) => r.title)
      const cols = Math.min(Math.max(Number(node?.columns) || 1, 1), 3)
      const perCol = Math.ceil(rowsText.length / cols) || 1
      const colW = (12.1 - 0.5 * (cols - 1)) / cols
      const rowH = Math.min(Math.max((areaBottom - areaY) / perCol, 0.5), 1.1)
      const tileW = 0.5
      const tileH = Math.min(rowH * 0.55, 0.42)
      rowsText.forEach((r, i) => {
        const ci = Math.floor(i / perCol)
        const ri = i % perCol
        const rx = 0.6 + ci * (colW + 0.5)
        const ry = areaY + ri * rowH
        slide.addShape('roundRect', {
          x: rx, y: ry, w: tileW, h: tileH,
          fill: { color: accentC }, line: { color: accentC, width: 0 }, rectRadius: 0.03,
        })
        slide.addText(String(i + 1).padStart(2, '0'), {
          x: rx, y: ry, w: tileW, h: tileH,
          fontSize: 12, bold: true, color: pptxHex(readableTextOn(theme?.accent || '#2563EB'), 'FFFFFF'),
          align: 'center', valign: 'middle',
        })
        const tx = rx + tileW + 0.18
        const tw = colW - tileW - 0.18
        slide.addText(r.title, {
          x: tx, y: ry - 0.02, w: tw, h: tileH + 0.04,
          fontSize: cols > 1 ? 13 : 16, bold: true, color: titleC, valign: 'middle', ...SHRINK_TO_FIT,
        })
        if (r.desc) {
          slide.addText(r.desc, {
            x: tx, y: ry + tileH + 0.02, w: tw, h: Math.max(rowH - tileH - 0.1, 0.22),
            fontSize: cols > 1 ? 10 : 12, italic: true, color: mutedC, valign: 'top', ...SHRINK_TO_FIT,
          })
        }
      })
      continue
    }

    // 'boxes' is a GRID of titled panels. Without this it fell through to the
    // generic body path, which concatenated every panel into one bullet list —
    // four panels of six bullets became a 24-line stream running off the slide.
    if (variant === 'boxes') {
      const kids = node?.children || []
      const cols = Math.min(Math.max(Number(node?.columns) || (kids.length <= 2 ? kids.length || 1 : kids.length <= 4 ? 2 : kids.length <= 6 ? 3 : 4), 1), 4)
      const rows = Math.ceil(kids.length / cols) || 1
      const gap = 0.22
      const gridW = 12.1
      const gridH = Math.max(areaBottom - areaY, 1)
      const cellW = (gridW - gap * (cols - 1)) / cols
      const cellH = (gridH - gap * (rows - 1)) / rows
      kids.forEach((cid, i) => {
        const cx = 0.6 + (i % cols) * (cellW + gap)
        const cy = areaY + Math.floor(i / cols) * (cellH + gap)
        slide.addShape('roundRect', {
          x: cx, y: cy, w: cellW, h: cellH,
          fill: { color: panelC }, line: { color: panelBorderC, width: 1 }, rectRadius: 0.06,
        })
        // First line of the panel is its heading (the specs write it as a bold
        // Markdown heading); the rest is the panel body.
        const panel: string[] = []
        collectText(cid, byId, resolve, panel)
        const plines = deMarkdown(panel.join('\n')).split('\n').filter(Boolean)
        const head = plines.shift() || ''
        slide.addText(head, {
          x: cx + 0.16, y: cy + 0.12, w: cellW - 0.32, h: 0.34,
          fontSize: 13, bold: true, color: titleC, valign: 'top', ...SHRINK_TO_FIT,
        })
        if (plines.length) {
          slide.addText(
            plines.map((t) => ({
              text: t.replace(/^•\s*/, ''),
              options: { breakLine: true, paraSpaceAfter: 4, bullet: /^•\s*/.test(t) ? { code: '2022', indent: 14 } : false },
            })),
            {
              x: cx + 0.16, y: cy + 0.5, w: cellW - 0.32, h: Math.max(cellH - 0.64, 0.3),
              fontSize: fitFontSize(plines, cellW - 0.32, Math.max(cellH - 0.64, 0.3), 10.5, 6, 4),
              color: bodyC, valign: 'top', lineSpacingMultiple: 1.1, ...SHRINK_TO_FIT,
            },
          )
        }
      })
      continue
    }

    // Text lines from the slide body (visual components are not text-extracted).
    // On 'kpi-split' the KeyValue children were ALREADY drawn as the tile band
    // above, so skip them here — collecting them again printed every tile a second
    // time as a "Label: value" bullet, and those extra lines overflowed the body
    // box and landed on top of the title.
    const out: string[] = []
    const bodyKids = (node?.children || []).filter(
      (c) => !(variant === 'kpi-split' && byId[c]?.component === 'KeyValue'),
    )
    bodyKids.forEach((c) => collectText(c, byId, resolve, out))
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

    // PROSE BESIDE A VISUAL — text on the left, the visual on the right. This is
    // NOT just 'two-column': every split-body variant needs it. Gating it on
    // 'two-column' alone meant a 'kpi-split' slide carrying bullets AND a chart
    // fell through to `if (chartNode)` below, which drew the chart full-width and
    // `continue`d — so the bullets were silently DROPPED from the download while
    // the on-screen slide showed them fine.
    const primaryVisual = chartNode ? 'chart' : diagramNode ? 'diagram' : null
    const splitBody = ['two-column', 'kpi-split', 'split', 'visual', 'content'].includes(
      variant.replace(/[_\s]/g, '-'),
    )
    const sideBySide = splitBody && primaryVisual && lines.length > 0

    if (sideBySide) {
      // `ratio` mirrors the renderer's body columns, so a chart-led 60/40 slide
      // exports chart-led rather than always splitting down the middle.
      const ratio = String(node?.ratio ?? '').trim()
      const textW = ratio === '40/60' ? 4.7 : ratio === '60/40' ? 7.1 : 5.85
      const gap = 0.4
      const visX = 0.7 + textW + gap
      const visW = 12.6 - visX
      const paras = lines.map((t) => ({
        text: t.replace(/^•\s*/, ''),
        options: { breakLine: true, paraSpaceAfter: 10, bullet: /^•\s*/.test(t) ? { code: '2022', indent: 16 } : false },
      }))
      slide.addText(paras, {
        x: 0.7, y: areaY, w: textW, h: areaH,
        fontSize: fitFontSize(paras.map((p) => p.text), textW, areaH, 16, 8, 10),
        color: bodyC, valign: 'top', align: 'left', lineSpacingMultiple: 1.25,
        ...SHRINK_TO_FIT,
      })
      if (primaryVisual === 'chart') addChartAt(visX, visW)
      else addDiagramAt(visX, visW)
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
      // Prose accompanying a table (the template's "table above, notes below"
      // pages) gets a band beneath it. Without this the table took the full area
      // and `continue`d, dropping the notes from the download entirely.
      const noteLines = lines.slice(0, 6)
      const noteH = noteLines.length ? Math.min(1.6, 0.3 + noteLines.length * 0.24) : 0
      const tableH = Math.max(areaH - noteH - (noteH ? 0.2 : 0), 1)
      slide.addTable(head.length ? [head, ...bodyRows] : bodyRows, {
        x: 0.6, y: areaY, w: 12.1, h: tableH,
        fontSize: rows.length > 6 || cols.length > 5 ? 11 : 13,
        color: bodyC, valign: 'middle',
        border: { type: 'solid', color: panelBorderC, pt: 1 },
        autoPage: false,
      })
      if (noteLines.length) {
        slide.addText(
          noteLines.map((t) => ({
            text: t.replace(/^•\s*/, ''),
            options: { breakLine: true, paraSpaceAfter: 4, bullet: /^•\s*/.test(t) ? { code: '2022', indent: 14 } : false },
          })),
          {
            x: 0.7, y: areaY + tableH + 0.15, w: 11.9, h: noteH,
            fontSize: fitFontSize(noteLines, 11.9, noteH, 12, 8, 4),
            color: bodyC, valign: 'top', lineSpacingMultiple: 1.15, ...SHRINK_TO_FIT,
          },
        )
      }
      continue
    }

    // Text body — vertically CENTERED (matches the renderer's justify-center) with
    // per-paragraph spacing; disc bullets where the source used a list, numbered
    // rows for variant='agenda' (mirrors the renderer's numbered agenda layout).
    if (lines.length) {
      const agenda = variant === 'agenda'
      const paras = lines.map((t) => {
        const isBullet = /^•\s*/.test(t)
        // On an agenda, PowerPoint supplies the number, so strip a leading "01 — "
        // the author wrote: keeping both produced "1. 01 — Country Overview".
        const text = agenda
          ? t.replace(/^•\s*/, '').replace(/^\d{1,2}\s*[—–-]\s*/, '')
          : t.replace(/^•\s*/, '')
        return {
          text,
          options: {
            breakLine: true,
            paraSpaceAfter: 12,
            bullet: agenda ? { type: 'number' as const, indent: 18 } : isBullet ? { code: '2022', indent: 18 } : false,
          },
        }
      })
      // `valign: 'top'`, not 'middle': a box that still overflows grows DOWNWARD
      // only, so the worst case clips at the bottom instead of printing over the
      // title. Centering is what put body text above the title.
      slide.addText(paras, {
        x: 0.7, y: areaY, w: 11.9, h: areaH,
        fontSize: fitFontSize(paras.map((p) => p.text), 11.9, areaH, 20),
        color: bodyC, valign: 'top', align: 'left', lineSpacingMultiple: 1.3,
        ...SHRINK_TO_FIT,
      })
    }
  }
  await pptx.writeFile({ fileName: filename })
}
