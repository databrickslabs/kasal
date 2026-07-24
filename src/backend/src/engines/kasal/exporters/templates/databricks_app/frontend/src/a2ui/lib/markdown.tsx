import type { Components } from 'react-markdown'

// Make inline citation markers clickable even when the model writes them as bare
// text (e.g. `...in October 2022.[1][2]`) and only puts the URLs in a trailing
// numbered "Sources" list. We read that list into an n→url map and rewrite any
// bare `[n]` in the body into a markdown link `[n](url)`; the renderer below then
// styles numeric-text links as superscript reference chips. No-op when there is
// no recognizable source list (so plain prose is untouched).
export function linkifyCitations(md: string): string {
  if (!md || md.indexOf('[') === -1) return md
  // Source-list lines: optional bullet, a number, then either `[Title](url)` or a
  // bare URL — e.g. `1. [Reuters](https://…)` or `- 2: https://…`.
  const srcLine =
    /^\s*(?:[-*]\s*)?\[?(\d{1,3})\]?\s*[.)\]:]\s+(?:\[[^\]]*\]\((https?:\/\/[^)\s]+)\)|<?(https?:\/\/[^\s>]+)>?)/gm
  const map: Record<string, string> = {}
  let m: RegExpExecArray | null
  while ((m = srcLine.exec(md)) !== null) {
    const n = m[1]
    const url = m[2] || m[3]
    if (url && !(n in map)) map[n] = url
  }
  if (Object.keys(map).length === 0) return md
  // Rewrite bare `[n]` (not already a link `[n](`) when we have a URL for n.
  return md.replace(/\[(\d{1,3})\](?!\()/g, (whole, n) => (map[n] ? `[${n}](${map[n]})` : whole))
}

// Shared ReactMarkdown component overrides, used by every markdown surface
// (the chat Prose and the A2UI Markdown component) so links behave the same.
//
// Two behaviours:
//   1. Every link opens in a new tab (rel=noopener) — required inside the
//      Databricks App iframe, where same-tab navigation would replace the app.
//   2. A link whose visible text is just a number — e.g. `[1](https://…)`, the
//      citation style the agent emits — renders as a small superscript chip you
//      click to open the source. Normal links render as normal links.
export const mdComponents: Components = {
  a({ node: _node, href, children, ...props }) {
    const text = (Array.isArray(children) ? children.join('') : String(children ?? '')).trim()
    const isCitation = /^\[?\d+\]?$/.test(text)
    if (isCitation) {
      const n = text.replace(/[[\]]/g, '')
      return (
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          title={href}
          className="ml-0.5 inline-flex items-center justify-center rounded bg-muted px-1 align-super text-[10px] font-medium leading-none text-muted-foreground no-underline transition-colors hover:bg-accent hover:text-foreground"
        >
          {n}
        </a>
      )
    }
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
        {children}
      </a>
    )
  },
}
