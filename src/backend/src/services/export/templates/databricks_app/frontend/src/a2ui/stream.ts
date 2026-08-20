// A2UI streaming — apply the messages a composer emits while it is still writing.
//
// The backend ships one message per completed piece of a surface (see
// `services/a2ui/stream.py`), following the A2UI protocol's streaming model:
// `createSurface`, then `updateComponents` / `updateDataModel` as each component
// and data key lands, and `deleteSurface` to retract a surface that turned out
// not to be deliverable.
//
// This works at all because A2UI components are an ADJACENCY LIST: a flat array
// whose parents name their children by id. `A2UIRenderer` looks every child up
// in a map and renders nothing for an id it does not have yet, so a slide can
// arrive before the chart it points at and simply fill in when that arrives. No
// ordering requirement, no tree surgery.
//
// Every function here is PURE and returns a new object — the store holds the
// result in React state, and mutating in place would not re-render.

import type { ComponentNode, Surface } from './types'

export interface CreateSurfaceMessage {
  createSurface: {
    surfaceId: string
    surfaceKind: string
    root: string
    components?: ComponentNode[]
    dataModel?: Record<string, unknown>
  }
}

export interface UpdateComponentsMessage {
  updateComponents: { surfaceId: string; components: ComponentNode[] }
}

export interface UpdateDataModelMessage {
  updateDataModel: { surfaceId: string; path: string; value: unknown }
}

export interface DeleteSurfaceMessage {
  deleteSurface: { surfaceId: string }
}

export type A2uiMessage = { version?: string } & Partial<
  CreateSurfaceMessage & UpdateComponentsMessage & UpdateDataModelMessage & DeleteSurfaceMessage
>

/** Decode a JSON Pointer segment back to its key (RFC 6901). */
function pointerKey(path: string): string {
  return path.replace(/^\//, '').replace(/~1/g, '/').replace(/~0/g, '~')
}

/**
 * Fold one streamed message into the surface built so far.
 *
 * Returns `null` for a retracted surface, and returns `surface` unchanged for a
 * message it cannot use — a malformed delta must never blank a surface the
 * reader is already looking at.
 */
export function applyA2uiMessage(surface: Surface | null, msg: A2uiMessage): Surface | null {
  if (!msg || typeof msg !== 'object') return surface

  if (msg.createSurface) {
    const m = msg.createSurface
    if (!m.root || !m.surfaceKind) return surface
    return {
      surfaceKind: m.surfaceKind,
      root: m.root,
      components: [...(m.components ?? [])],
      dataModel: { ...(m.dataModel ?? {}) },
    }
  }

  if (msg.deleteSurface) return null

  // Per the protocol a surface must exist before anything updates it. A stray
  // update is dropped rather than used to invent one, because a surface with no
  // root would render as an empty shell.
  if (!surface) return surface

  if (msg.updateComponents) {
    const incoming = msg.updateComponents.components
    if (!Array.isArray(incoming) || incoming.length === 0) return surface
    const components = [...surface.components]
    const indexById = new Map(components.map((c, i) => [c.id, i]))
    for (const node of incoming) {
      if (!node || typeof node.id !== 'string') continue
      const at = indexById.get(node.id)
      // Replace by id, never append a duplicate: this is what lets the real
      // slides overwrite the outline skeleton's placeholders in place.
      if (at === undefined) {
        indexById.set(node.id, components.length)
        components.push(node)
      } else {
        components[at] = node
      }
    }
    return { ...surface, components }
  }

  if (msg.updateDataModel) {
    const { path, value } = msg.updateDataModel
    if (typeof path !== 'string') return surface
    if (path === '/' || path === '') {
      return {
        ...surface,
        dataModel: (value && typeof value === 'object' ? { ...(value as object) } : {}) as Record<
          string,
          unknown
        >,
      }
    }
    const dataModel = { ...(surface.dataModel ?? {}) }
    const key = pointerKey(path)
    if (value === null) delete dataModel[key]
    else dataModel[key] = value
    return { ...surface, dataModel }
  }

  return surface
}

/** Fold a whole message list — the replay used on reconnect and in tests. */
export function applyA2uiMessages(
  surface: Surface | null,
  messages: A2uiMessage[],
): Surface | null {
  return (messages ?? []).reduce<Surface | null>(applyA2uiMessage, surface)
}

/**
 * Is this surface still being written?
 *
 * True when the root exists but something it (transitively) points at does not
 * yet, or when any node is still flagged `pending` by the outline skeleton. The
 * renderer uses it to show a slide as "being written" rather than as blank, and
 * the chat uses it to hold back the "done" affordances.
 */
export function isSurfaceStreaming(surface: Surface | null): boolean {
  if (!surface) return false
  const byId = new Map(surface.components.map((c) => [c.id, c]))
  if (!byId.has(surface.root)) return true
  const seen = new Set<string>()
  const stack = [surface.root]
  while (stack.length) {
    const id = stack.pop() as string
    if (seen.has(id)) continue
    seen.add(id)
    const node = byId.get(id)
    if (!node) return true // referenced but not delivered yet
    if (node.pending === true) return true
    for (const child of (node.children as string[] | undefined) ?? []) stack.push(child)
  }
  return false
}
