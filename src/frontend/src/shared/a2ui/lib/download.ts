// Client-side download helpers for A2UI surfaces: CSV (tables) and PNG
// snapshots (dashboards). Heavy libs are imported dynamically so they only
// load when a download is actually triggered.

import type { ComponentNode } from '../types'

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
