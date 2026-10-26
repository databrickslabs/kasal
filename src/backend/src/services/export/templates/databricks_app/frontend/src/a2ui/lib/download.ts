// Client-side download helpers for A2UI surfaces: CSV (tables) and PNG
// snapshots (dashboards). Heavy libs are imported dynamically so they only
// load when a download is actually triggered.


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
