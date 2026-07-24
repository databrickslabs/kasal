import type { Surface } from './a2ui/types'

export interface AgentReply {
  text: string
  a2ui?: Surface
}

// Calls the agent server's Responses endpoint. Same-origin in production (served
// behind the agent's chat proxy); proxied to the backend in dev (see vite.config).
export async function sendMessage(
  text: string,
  conversationId: string,
  signal?: AbortSignal,
  mode?: string,
): Promise<AgentReply> {
  const res = await fetch('/invocations', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      input: [{ role: 'user', content: text }],
      context: { conversation_id: conversationId },
      // Answer depth picked in the UI: chat | research | deep (see agent.py).
      // Also pass the conversation id as custom_inputs.session_id: the server's
      // get_session_id() reads request.context.conversation_id first but falls
      // back to this, which the MLflow ResponsesAgent request always surfaces.
      // Without a stable session id the server can't key per-conversation history
      // (no memory) or the out-of-band A2UI surface (no presentation), so this is
      // what makes both work end-to-end.
      custom_inputs: { mode: mode || 'research', session_id: conversationId },
    }),
    signal,
  })
  if (!res.ok) throw new Error(`Agent error ${res.status}: ${await res.text()}`)
  const data = await res.json()
  const out: string = (data.output ?? [])
    .flatMap((it: any) => it.content ?? [])
    .filter((c: any) => c.type === 'output_text' || c.type === 'text')
    .map((c: any) => c.text ?? '')
    .join('')
  const a2ui = data.custom_outputs?.a2ui as Surface | undefined
  return { text: out, a2ui }
}

export interface A2uiPoll {
  // pending: composing · ready: surface available · none: no rich surface for this
  // turn · idle: unknown (not started / pruned).
  status: 'pending' | 'ready' | 'none' | 'idle'
  surface?: Surface
}

// Poll for this turn's A2UI surface. The agent composes it out-of-band (so the
// answer request returns fast and never times out behind the Databricks Apps
// proxy), stashing it for this endpoint — the UI polls until ready/none.
export async function fetchA2ui(
  conversationId: string,
  signal?: AbortSignal,
): Promise<A2uiPoll> {
  try {
    const res = await fetch(`/a2ui/${encodeURIComponent(conversationId)}`, { signal })
    if (!res.ok) return { status: 'idle' }
    const data = await res.json()
    return {
      status: (data?.status as A2uiPoll['status']) ?? 'idle',
      surface: (data?.surface as Surface | undefined) ?? undefined,
    }
  } catch {
    return { status: 'idle' }
  }
}

// Ask the backend to stop the running turn for this conversation. Cooperative:
// the crew aborts at the next step boundary so token spend stops. Best-effort.
export async function cancelTurn(conversationId: string): Promise<void> {
  try {
    await fetch(`/cancel/${encodeURIComponent(conversationId)}`, { method: 'POST' })
  } catch {
    /* the local abort already stopped the UI; ignore network errors */
  }
}

// Ephemeral "what is the agent doing right now" — polled only while a turn is in
// flight. Returns a short status string (e.g. "Using tool: …") or null when idle.
export async function fetchProgress(
  conversationId: string,
  signal?: AbortSignal,
): Promise<string | null> {
  try {
    const res = await fetch(`/progress/${encodeURIComponent(conversationId)}`, { signal })
    if (!res.ok) return null
    const data = await res.json()
    return (data?.status as string | null) ?? null
  } catch {
    return null
  }
}
