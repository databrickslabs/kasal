import { useContext, useMemo } from 'react'
import type { Surface } from './types'
import { resolveValue } from './resolve'
import { registry, Unsupported } from './registry'
import { SurfaceContext, SurfaceChromeContext } from './lib/surfaceContext'

// Surface kind -> container className. The ONLY place surfaceKind is interpreted;
// unknown kinds fall back to the document container.
const SURFACE_CLASS: Record<string, string> = {
  conversation: 'flex flex-col gap-2.5',
  document: 'flex flex-col gap-2.5',
  presentation: '',
  dashboard: '',
  mindmap: 'overflow-x-auto',
  quiz: '',
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

  // In fit mode the host wants the (deck) surface to fill the available height so
  // it can letterbox to fit; make this container a flex column that fills, so the
  // height chain reaches the SlideDeck. Non-fit keeps the natural per-kind class.
  const { fit } = useContext(SurfaceChromeContext)
  const baseClass = SURFACE_CLASS[payload.surfaceKind] ?? 'flex flex-col gap-2.5'
  const containerClass = fit ? `${baseClass} flex flex-col flex-1 min-h-0`.trim() : baseClass
  return (
    <SurfaceContext.Provider value={payload}>
      <div className={containerClass}>{render(payload.root)}</div>
    </SurfaceContext.Provider>
  )
}
