import { useContext, useMemo } from 'react'
import type { Surface } from './types'
import { resolveValue } from './resolve'
import { registry, Unsupported } from './registry'
import { SurfaceDownloadMenu } from './components'
import { SurfaceContext, SurfaceChromeContext } from './lib/surfaceContext'

// Surface kind -> container className. The ONLY place surfaceKind is interpreted;
// unknown kinds fall back to the document container.
const SURFACE_CLASS: Record<string, string> = {
  conversation: 'flex flex-col gap-2.5',
  document: 'flex flex-col gap-2.5',
  dashboard: '',
  mindmap: 'overflow-x-auto',
  quiz: '',
  flashcards: '',
  map: '',
}

export function A2UIRenderer({ payload }: { payload: Surface }) {
  const byId = useMemo(
    () => Object.fromEntries((payload.components || []).map((c) => [c.id, c])),
    [payload],
  )
  const resolve = (v: unknown) => resolveValue(v, payload.dataModel ?? {})

  const render = (id: string): JSX.Element => {
    const node = byId[id]
    if (!node) return <></>
    const Comp = registry[node.component] ?? Unsupported
    return <Comp key={id} node={node} render={render} resolve={resolve} />
  }

  // In fit mode the host wants the surface to fill the available height so it
  // can letterbox to fit. Non-fit keeps the natural per-kind class.
  const { fit } = useContext(SurfaceChromeContext)
  const baseClass = SURFACE_CLASS[payload.surfaceKind] ?? 'flex flex-col gap-2.5'
  const containerClass = fit ? `${baseClass} flex flex-col flex-1 min-h-0`.trim() : baseClass
  // Every surface (dashboard, document, quiz) gets the same elegant download
  // menu (PDF) — rendered as nothing when downloads are off (preview pane /
  // PDF rasterizer) or unwired. (Legacy SlideDeck surfaces render Unsupported —
  // presentations moved to the chat HTML path.)
  return (
    <SurfaceContext.Provider value={payload}>
      <div className={containerClass}>
        <SurfaceDownloadMenu className="mb-1" />
        {render(payload.root)}
      </div>
    </SurfaceContext.Provider>
  )
}
