/**
 * Diagram → native PowerPoint shapes, for the PPTX export.
 *
 * Split out of `download.ts`, which had grown past the 800-line ceiling. This is
 * the natural seam: it renders one component (`Diagram`) into pptxgenjs shapes per
 * archetype, and the rest of the exporter needs only `addDiagramToSlide`.
 *
 * Editable shapes rather than a rasterised image, so an exported deck can be
 * reworked in PowerPoint instead of carrying a flat picture of a diagram.
 */
import type { ComponentNode } from '../types'
import { readableTextOn, seriesFromAccent } from './deckThemes'

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
export const pptxHex = (c: string, fallback: string) => {
  const m = /^#?([0-9a-fA-F]{6})$/.exec((c || '').trim())
  return m ? m[1] : fallback
}

export function addDiagramToSlide(
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
