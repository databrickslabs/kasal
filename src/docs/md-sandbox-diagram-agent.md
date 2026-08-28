# %md-sandbox Diagram Agent

Kasal chat renders any assistant message that contains a fenced ` ```html ` (or
` ```svg `) block as a **live diagram** inside a sandboxed iframe, with a
**Copy %md-sandbox cell** button that yields a Databricks `%md-sandbox` notebook
cell (see `components/ChatMode/components/Chat/HtmlDiagramBlock.tsx` and
`components/ChatMode/utils/mdSandboxDiagram.ts`).

Rendering is automatic — the trigger is purely the ` ```html ` / ` ```svg `
fence in the assistant text. What makes an agent *produce* that fence is its
**prompt**. There is no global system prompt in Kasal; each agent is shaped by
its `role` / `goal` / `backstory` (CrewAI-style). To get diagrams, put the
instruction below in the agent's **`backstory`** (or `goal`, or `system_template`
for full control).

## Drop-in prompt (paste into an agent's `backstory`)

> You are a Databricks `%md-sandbox` diagram specialist. When the user asks for a
> diagram or visual, respond with clean, self-contained HTML that renders inside
> a Databricks `%md-sandbox` notebook cell.
>
> Rules for every diagram:
> - Wrap the whole thing in a single fenced ` ```html ` code block. A brief
>   one-line intro before it is fine. Do NOT include the `%md-sandbox` magic line
>   — the app adds it on copy.
> - The HTML must be fully self-contained: inline `<style>` and inline `<svg>`
>   only. No external URLs, no `<img src>` to remote files, no CDN scripts, no web
>   fonts. Use a system font stack (`-apple-system, "Segoe UI", Roboto, sans-serif`).
> - Build the diagram primarily from inline SVG (boxes, arrows, labels) with a
>   wrapping `<div>` for legends/captions, like a hand-authored architecture
>   diagram. A small inline `<script>` for optional polish (hover, tabs) is fine,
>   but the diagram must read clearly with no JS.
> - Scope all CSS under a single wrapper class (e.g. `.scn`) so styles never leak.
> - Do NOT put blank lines anywhere inside the HTML. `%md-sandbox` runs the cell
>   through a markdown processor first, and a blank line inside an inline-HTML
>   block terminates it (the SVG then renders as loose text). Keep every line
>   contiguous — indentation and comments are fine, empty lines are not.
> - Keep text legible: font-size >= 14px, adequate contrast.
> - On a follow-up request, modify the previous diagram and return the FULL
>   updated ` ```html ` block again (never a diff or partial snippet).

## Notes

- **Live preview while streaming:** an *unclosed* ` ```html ` fence is treated as
  "still building" — the iframe re-mounts (throttled) as tokens arrive, so the
  user watches the SVG take shape. It flips to "Rendered diagram" once the
  closing ` ``` ` arrives.
- **Security:** the iframe is sandboxed without `allow-same-origin` and carries a
  CSP that blocks network egress (`connect-src 'none'`), so agent-authored HTML
  cannot reach the app or exfiltrate data.
- **`svg` also works:** a ` ```svg ` block renders the same way.
- **No backend change** is required — the whole capability is the agent prompt
  plus the frontend renderer.

## ChatMode specifics (routing, palette, trace)

In ChatMode the directive is applied automatically to the light agent, and three
behaviors are engine-side:

- **Routing is word-boundary matched, not substring.** A turn is owned by the
  HTML renderer (and A2UI composition disabled) only when the request actually
  asks for a presentation/slides/deck/diagram — `deck_intent` /
  `html_owned_intent` in `services/a2ui/compose.py`. Incidental mentions
  ("how do slide-out panels work?", "what's on deck today?") stay on the normal
  A2UI path. Deck follow-ups ("make slide 3 blue", "change the title slide")
  stay on the HTML path.
- **Deck colors come from the workspace palette.** When the turn is a deck
  request, the chat service loads the UIConfig themes (Configuration → UI) and
  the slide-design template is generated from the `presentation` palette
  (accent/background/surface/text/heading/muted), with auto-contrast cover text
  — so HTML decks and A2UI decks share one design. Defaults apply only when no
  palette is configured.
- **The run's trace records the HTML path.** An `html_intent` trace event marks
  the moment the renderer takes ownership (and why A2UI is skipped), and an
  `html_surface` event records what was produced (`rendered`, `deck`, `slides`,
  `bytes`) — so a deck turn is no longer a single opaque `llm_call` in the
  timeline.
