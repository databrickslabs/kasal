/**
 * `SurfaceDownloadMenu` — the shared Download dropdown for ANY A2UI surface.
 *
 * Split out of `slides.tsx`, which had grown past the 800-line ceiling. This is a
 * clean seam: the menu is surface-agnostic chrome (it self-detects what the surface
 * can export), so it never belonged in the slide renderer beyond having been
 * written there first.
 */
import { useCallback, useContext, useEffect, useRef, useState } from 'react'
import { ChevronDown, Download, FileText } from 'lucide-react'
import { Button } from '../ui/button'
import { SurfaceContext, SurfaceChromeContext } from '../lib/surfaceContext'
import { cn } from '../lib/utils'

// ---- SurfaceDownloadMenu (shared download chrome) --------------------------
// One elegant "Download" dropdown reused by every surface. Self-detects what the
// surface can export: PDF (any surface, when the host wires `onDownloadPdf`). Renders
// nothing when downloads are suppressed or there's nothing to offer — so it's
// safe to drop into the renderer for ALL surfaces.
export function SurfaceDownloadMenu({ className }: { className?: string }) {
  const surface = useContext(SurfaceContext)
  const { downloads: showDownloads, onDownloadPdf } = useContext(SurfaceChromeContext)
  const [open, setOpen] = useState(false)
  const [exporting, setExporting] = useState(false)
  if (!showDownloads) return null
  const options: { key: string; label: string; sub: string; icon: JSX.Element; onClick: () => void }[] = []
  if (onDownloadPdf) options.push({ key: 'pdf', label: 'PDF', sub: 'Portable document', icon: <FileText className="size-4" />, onClick: onDownloadPdf })
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
