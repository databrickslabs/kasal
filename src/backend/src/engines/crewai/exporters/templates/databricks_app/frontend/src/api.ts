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
      custom_inputs: { mode: mode || 'research' },
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
