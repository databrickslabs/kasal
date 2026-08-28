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
