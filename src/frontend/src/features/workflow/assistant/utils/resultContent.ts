import { toSurface } from '../../../chat/utils/surfaceAdapter';
import { hasDiagram, splitDiagramSegments } from '../../../chat/utils/mdSandboxDiagram';

// Results can be fragments (often starting with an HTML comment), not just
// full documents. Only recognize markup at the beginning: prose mentioning a
// tag must remain prose. Rendering still goes through Chat's isolated iframe.
const isHtmlOutput = (content: string) => /^(?:\s|<!--[\s\S]*?-->)*(?:<!doctype\s+html\b|<(?:html|head|body|style|div|section|main|article|svg|table|form|header|h[1-6])(?:\s|>))/i.test(content);

export function normalizeBuilderHtml(content: string): string {
  if (isHtmlOutput(content)) return `\`\`\`html\n${content}\n\`\`\``;
  // Some models omit the language on a fenced HTML deliverable.
  return content.replace(/(^|\n)([ \t]*)```[ \t]*\n([\s\S]*?)(\n[ \t]*```(?=\s|$)|$)/g,
    (match, prefix, indent, body, close) => isHtmlOutput(body)
      ? `${prefix}${indent}\`\`\`html\n${body}${close}` : match);
}

export const hasRichHtml = (content: string) => hasDiagram(splitDiagramSegments(normalizeBuilderHtml(content)));

/** Keep the rich-result envelope intact when output and a2ui are siblings. */
export function builderResultContent(result: unknown): string | null {
  if (result == null) return null;
  let value = result;
  if (!toSurface(result) && typeof result === 'object' && !Array.isArray(result)) {
    const output = (result as Record<string, unknown>).output;
    if (output != null) value = output;
  }
  return typeof value === 'string' ? value : JSON.stringify(value, null, 2);
}
