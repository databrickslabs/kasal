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

    if (variant === 'title' || variant === 'section') {
      if (kicker) slide.addText(kicker.toUpperCase(), { x: 0.6, y: 2.0, w: 12.1, h: 0.4, fontSize: 13, bold: true, color: kickerC, align: 'center', charSpacing: 2 })
      if (title) slide.addText(title, { x: 0.8, y: 2.5, w: 11.7, h: 1.8, fontSize: 40, bold: true, color: titleC, align: 'center' })
      slide.addShape('rect', { x: 5.92, y: 4.5, w: 1.5, h: 0.06, fill: { color: accentC } })
      const sub = String(resolve(node?.subtitle) ?? '').trim()
      if (sub) slide.addText(sub, { x: 1.5, y: 4.8, w: 10.3, h: 1.2, fontSize: 18, color: mutedC, align: 'center' })
      continue
    }

    let y = 0.5
    if (kicker) {
      slide.addText(kicker.toUpperCase(), { x: 0.6, y, w: 12.1, h: 0.35, fontSize: 12, bold: true, color: kickerC, charSpacing: 2 })
      y += 0.45
    }
    if (title) {
      slide.addText(title, { x: 0.6, y, w: 12.1, h: 0.9, fontSize: 28, bold: true, color: titleC })
      y += 0.95
    }
    slide.addShape('rect', { x: 0.62, y: y - 0.12, w: 0.9, h: 0.06, fill: { color: accentC } })
    y += 0.2

    if (variant === 'stats') {
      const kvs = (node?.children || []).map((id) => byId[id]).filter((n) => n && n.component === 'KeyValue')
      const n = Math.max(kvs.length, 1)
      const tileW = 12.1 / n
      kvs.forEach((kv, i) => {
        const vx = 0.6 + i * tileW
        slide.addText(String(resolve(kv.value) ?? ''), { x: vx, y: 3.0, w: tileW - 0.25, h: 1.0, fontSize: 40, bold: true, color: accentC })
        slide.addText(String(resolve(kv.label) ?? ''), { x: vx, y: 4.1, w: tileW - 0.25, h: 0.8, fontSize: 14, color: mutedC })
      })
      continue
    }

    const out: string[] = []
    ;(node?.children || []).forEach((c) => collectText(c, byId, resolve, out))
    const text = deMarkdown(out.join('\n'))
    if (text) {
      slide.addText(text, { x: 0.6, y: y + 0.1, w: 12.1, h: 7.0 - y, fontSize: 16, color: bodyC, valign: 'top', lineSpacingMultiple: 1.12 })
    }
  }
  await pptx.writeFile({ fileName: filename })
}
