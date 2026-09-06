import { toSurface } from './surfaceAdapter';
import type { PreviewContent } from '../types/preview';

/**
 * Strip a leading `**Task name**\n\n` prefix the chat layer prepends to
 * task-output messages. The prefix is for chat display only; the preview
 * should render just the body.
 */
export function stripTaskTitlePrefix(raw: string): string {
  const match = raw.match(/^\s*\*\*[^\n*][^\n]*?\*\*\s*\n\n+/);
  return match ? raw.slice(match[0].length) : raw;
}

/**
 * Strip markdown code fences if present.
 * Handles: ```html\n...\n``` or ```json\n...\n``` etc.
 */
function stripCodeFences(raw: string): string {
  const trimmed = raw.trim();
  const fenceMatch = trimmed.match(/^```\w*\s*\n([\s\S]*?)\n\s*```\s*$/);
  if (fenceMatch) {
    return fenceMatch[1];
  }
  // Also handle when there's text before/after the code fence
  const innerMatch = trimmed.match(/```(?:html|json|xml)\s*\n([\s\S]*?)\n\s*```/);
  if (innerMatch) {
    return innerMatch[1];
  }
  return raw;
}

/**
 * A task output is previewable IFF it is (or contains) a parseable A2UI
 * document. Everything else — HTML, generic JSON, markdown, plain text —
 * returns null and stays in the chat transcript.
 */
export function parsePreviewContent(raw: string): PreviewContent | null {
  if (!raw || raw.length < 10) return null;

  // Drop the chat layer's bold-title prefix so the preview shows only the body.
  const body = stripTaskTitlePrefix(raw);

  // Strip markdown code fences that often wrap the JSON.
  const cleaned = stripCodeFences(body);

  // toSurface accepts the new {text,a2ui} envelope, a bare Surface, a JSON string
  // of either, or an older legacy document found anywhere in the tree (adapted).
  // PreviewContent.data always holds the canonical NEW Surface JSON afterwards.
  const surface = toSurface(cleaned);
  return surface ? { type: 'ui', data: JSON.stringify(surface) } : null;
}

