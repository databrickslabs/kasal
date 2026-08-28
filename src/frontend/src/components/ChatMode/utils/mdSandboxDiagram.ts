/**
 * Helpers for rendering agent-authored, self-contained HTML/SVG diagrams in chat
 * and copying them as a Databricks `%md-sandbox` notebook cell.
 *
 * The agent emits a fenced ```html (or ```svg) block of inline-CSS + inline-SVG.
 * The chat renders that block live in a sandboxed iframe (see HtmlDiagramBlock),
 * and the "Copy %md-sandbox cell" button yields a cell that renders identically
 * when pasted into a Databricks notebook.
 */

/**
 * Databricks runs a `%md-sandbox` cell through a markdown processor BEFORE
 * rendering the inline HTML. A blank line inside an inline-HTML block terminates
 * it there, so the SVG after it renders as loose text with the shapes gone.
 * Strip blank lines to keep the whole block contiguous (indentation is fine).
 */
export function notebookSafe(html: string): string {
  return html
    .split('\n')
    .filter((line) => line.trim() !== '')
    .join('\n');
}

/** Build the exact `%md-sandbox` cell to copy into a Databricks notebook. */
export function buildMdSandboxCell(html: string): string {
  return `%md-sandbox\n${notebookSafe(html)}`;
}

export type DiagramSegment =
  | { type: 'text'; text: string }
  | { type: 'diagram'; code: string; lang: 'html' | 'svg'; closed: boolean };

// Opening fence for a diagram block: ```html / ```svg (optionally with trailing
// spaces) followed by a newline. Only these two languages are treated as
// diagrams — every other fence (```python, a bare ```, ...) stays plain code.
const OPEN_FENCE = /```(html|svg)[ \t]*\r?\n/i;
// A closing ``` fence sitting on its own line.
const CLOSE_FENCE = /\r?\n[ \t]*```[ \t]*(?:\r?\n|$)/;

/**
 * Split streamed/complete assistant content into ordered text + diagram
 * segments. Handles multiple diagram blocks and — critically for streaming — a
 * trailing UNCLOSED block (still being written), which is returned with
 * `closed: false` so the UI can show a live "building" preview.
 */
export function splitDiagramSegments(content: string): DiagramSegment[] {
  const segments: DiagramSegment[] = [];
  let rest = content;
  // Guard against pathological input; real messages have a handful of blocks.
  for (let guard = 0; rest && guard < 1000; guard++) {
    const open = rest.match(OPEN_FENCE);
    if (!open || open.index === undefined) {
      segments.push({ type: 'text', text: rest });
      break;
    }
    const before = rest.slice(0, open.index);
    if (before) segments.push({ type: 'text', text: before });

    const lang = open[1].toLowerCase() as 'html' | 'svg';
    const afterOpen = rest.slice(open.index + open[0].length);
    const close = afterOpen.match(CLOSE_FENCE);
    if (!close || close.index === undefined) {
      // Unclosed fence — the rest is a diagram body still streaming in.
      segments.push({ type: 'diagram', code: afterOpen, lang, closed: false });
      break;
    }
    segments.push({ type: 'diagram', code: afterOpen.slice(0, close.index), lang, closed: true });
    rest = afterOpen.slice(close.index + close[0].length);
  }
  return segments;
}

/** True when the content carries at least one renderable diagram block. */
export function hasDiagram(segments: DiagramSegment[]): boolean {
  return segments.some((s) => s.type === 'diagram');
}
