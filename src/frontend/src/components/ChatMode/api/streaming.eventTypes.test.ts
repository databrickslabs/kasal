import { describe, expect, it } from 'vitest'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

/**
 * The SSE event whitelist must cover everything the stream hook handles.
 *
 * `EventSource` delivers a NAMED event only to a listener registered for that
 * exact name — `onmessage` receives unnamed frames and nothing else. So the
 * `eventTypes` array in `streaming.ts` is not documentation, it is a filter, and
 * an event missing from it is dropped by the browser before a single line of
 * application code runs.
 *
 * That failure is close to invisible: the server broadcasts happily and its logs
 * show the event going out, the hook has a perfectly good `case` for it, and
 * nothing anywhere reports a problem. `a2ui_delta` was emitted, logged
 * server-side, and silently discarded by every browser for the whole of a
 * feature's development.
 */
// Resolved from the vitest root rather than import.meta.url, which does not
// survive the transform here.
const CHAT_MODE = join(process.cwd(), 'src/components/ChatMode')

function read(rel: string): string {
  return readFileSync(join(CHAT_MODE, rel), 'utf8')
}

/** The names passed to `addEventListener` for the execution stream. */
function whitelistedEventTypes(): string[] {
  const src = read('api/streaming.ts')
  const block = src.slice(src.indexOf('const eventTypes = ['))
  const list = block.slice(0, block.indexOf(']'))
  return [...list.matchAll(/'([a-z0-9_]+)'/g)].map((m) => m[1])
}

/** The event names the execution-stream hook actually switches on. */
function handledEventTypes(): string[] {
  const src = read('hooks/useExecutionStream.ts')
  const block = src.slice(src.indexOf('switch (event.event)'))
  return [...block.matchAll(/case '([a-z0-9_]+)':/g)].map((m) => m[1])
}

describe('SSE execution-stream event types', () => {
  it('reads both lists', () => {
    expect(whitelistedEventTypes().length).toBeGreaterThan(3)
    expect(handledEventTypes().length).toBeGreaterThan(3)
  })

  it('registers a listener for every event the hook handles', () => {
    // 'error' is deliberately outside the loop in streaming.ts — EventSource
    // fires it for both a server-sent error frame and a native transport
    // failure, so it gets its own listener.
    const whitelisted = new Set([...whitelistedEventTypes(), 'error'])
    const unreachable = handledEventTypes().filter((t) => !whitelisted.has(t))

    expect(unreachable,
      `these have a case in useExecutionStream but no addEventListener in ` +
      `streaming.ts, so the browser drops them before the hook is reached: ` +
      `${unreachable.join(', ')}`,
    ).toEqual([])
  })

  it('carries a2ui_delta specifically', () => {
    // The one that was missing. Named explicitly so deleting it fails loudly
    // rather than merely shrinking a set.
    expect(whitelistedEventTypes()).toContain('a2ui_delta')
  })

  it('keeps the whitelist a superset, never a subset', () => {
    // The direction that matters. An extra listener is harmless — 'hitl_request'
    // is consumed elsewhere, not by this switch — but a MISSING one silently
    // drops the event, which is the failure this file exists to prevent.
    const whitelisted = new Set([...whitelistedEventTypes(), 'error'])
    for (const handled of handledEventTypes()) {
      expect(whitelisted.has(handled), `no listener for '${handled}'`).toBe(true)
    }
  })
})
