import { useMemo } from 'react'
import type { Surface } from './types'
import { resolveValue } from './resolve'
import { registry, Unsupported } from './registry'

// Surface kind -> container className. The ONLY place surfaceKind is interpreted;
// unknown kinds fall back to the document container.
const SURFACE_CLASS: Record<string, string> = {
  conversation: 'flex flex-col gap-2.5',
  document: 'flex flex-col gap-2.5',
  presentation: '',
  dashboard: '',
  mindmap: 'overflow-x-auto',
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

  const containerClass = SURFACE_CLASS[payload.surfaceKind] ?? 'flex flex-col gap-2.5'
  return <div className={containerClass}>{render(payload.root)}</div>
}
