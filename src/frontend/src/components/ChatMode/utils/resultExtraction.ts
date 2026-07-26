/**
 * Pulling the answer out of a completed run's payload.
 * 
 * A run result may carry plain text, an embedded A2UI document, or both; these
 * decide what the chat shows as prose and what it hands to the renderer.
 */
import { parseUiDocument, extractDocSummary } from './surfaceAdapter';
import type { Surface } from '../../../shared/a2ui';

export function stripEmbeddedUiDocument(text: string): string {
  if (!text || (!text.includes('createSurface') && !text.includes('updateComponents'))) {
    return text;
  }
  // The model's own chat one-liner (top-level "summary" in the doc), if present;
  // else the generic line. Used for both the whole-doc and embedded-doc cases.
  const friendly = extractDocSummary(text) || 'Generated an app. View it in the preview pane.';
  // The common case: the WHOLE payload IS the UI document (no surrounding prose).
  // The preview pane renders it, so collapse the chat to the friendly line rather
  // than surgically excising brackets — brittle for a weak model's slightly
  // malformed JSON. parseUiDocument is repair-tolerant (coerceJson rebalances
  // mismatched brackets), so e.g. gpt-5-nano's invalid A2UI is recognised here
  // too. Gated on the text STARTING with the document so a genuine prose answer
  // that merely embeds a doc keeps its prose (handled by the per-block logic below).
  const whole = text.trim();
  if ((whole.startsWith('{') || whole.startsWith('[')) && parseUiDocument(whole)) {
    return friendly;
  }
  let cleaned = text;
  // 1. Remove fenced ```json blocks that parse as a UI document.
  cleaned = cleaned.replace(/```(?:json)?\s*([\s\S]*?)```/g, (match, inner) =>
    parseUiDocument(inner.trim()) ? '' : match,
  );
  // 2. Remove a bare (unfenced) JSON document — from the first '{' that opens
  //    a parseable UI document through its matching closing brace.
  if (cleaned.includes('createSurface') || cleaned.includes('updateComponents')) {
    const start = cleaned.indexOf('{');
    if (start >= 0) {
      let depth = 0;
      for (let i = start; i < cleaned.length; i++) {
        if (cleaned[i] === '{') depth++;
        else if (cleaned[i] === '}') {
          depth--;
          if (depth === 0) {
            const candidate = cleaned.slice(start, i + 1);
            if (parseUiDocument(candidate)) {
              cleaned = cleaned.slice(0, start) + cleaned.slice(i + 1);
            }
            break;
          }
        }
      }
    }
  }
  // Drop a now-orphaned "json" fence label and tidy whitespace.
  cleaned = cleaned.replace(/(^|\n)\s*json\s*(\n|$)/g, '$1').replace(/\n{3,}/g, '\n\n').trim();
  return cleaned || friendly;
}

/**
 * Extract the final answer text from the various result shapes the backend
 * returns ("text", {result}, {result:{result}}, {content}, {output}, {value}…).
 * Shared by the live SSE completion path AND the REST polling fallback so both
 * render the answer identically.
 */
export function extractResultText(data: Record<string, unknown>): string {
  let resultText = '';
  try {
    const rawResult = data.result;
    if (typeof rawResult === 'string') {
      try {
        const parsed = JSON.parse(rawResult);
        if (parsed && typeof parsed === 'object') {
          resultText = (typeof parsed.result === 'string' ? parsed.result : '')
            || (typeof parsed.content === 'string' ? parsed.content : '')
            || (typeof parsed.value === 'string' ? parsed.value : '')
            // The composed light/crew envelope is { text, a2ui } — the chat shows
            // `text`; the a2ui surface is pulled out separately (extractA2uiSurface).
            || (typeof parsed.text === 'string' ? parsed.text : '')
            || rawResult;
        } else {
          resultText = rawResult;
        }
      } catch {
        resultText = rawResult;
      }
    } else if (rawResult && typeof rawResult === 'object') {
      const nested = rawResult as Record<string, unknown>;
      // `nested.text` covers the composed { text, a2ui } envelope (the a2ui surface
      // is extracted separately by extractA2uiSurface).
      const inner = nested.result ?? nested.content ?? nested.raw ?? nested.value ?? nested.text;
      if (typeof inner === 'string') {
        resultText = inner;
      } else if (inner && typeof inner === 'object') {
        const deepContent = (inner as Record<string, unknown>).content;
        if (typeof deepContent === 'string') {
          resultText = deepContent;
        } else {
          resultText = JSON.stringify(inner);
        }
      } else {
        resultText = JSON.stringify(nested);
      }
    }
    if (!resultText && typeof data.content === 'string') {
      resultText = data.content;
    }
    if (!resultText) {
      const output = data.output;
      resultText = typeof output === 'string' ? output : '';
    }
  } catch {
    resultText = '';
  }
  // The preview pane renders A2UI surfaces — never show their raw JSON in chat.
  return stripEmbeddedUiDocument(resultText);
}

/**
 * Pull the composed A2UI surface out of a completion payload, if any. The
 * light/crew runners persist a rich answer as { text, a2ui } (a plain chat turn
 * stays a bare string), so this returns the surface for inline rendering or null
 * when the turn is plain prose. Tolerates the result arriving as a JSON string
 * or nested one level (result.result), matching extractResultText's unwrapping.
 */
export function extractA2uiSurface(data: Record<string, unknown>): Surface | null {
  try {
    let result: unknown = data.result;
    if (typeof result === 'string') {
      try {
        result = JSON.parse(result);
      } catch {
        return null;
      }
    }
    if (!result || typeof result !== 'object') return null;
    const obj = result as Record<string, unknown>;
    const nested =
      obj.result && typeof obj.result === 'object'
        ? (obj.result as Record<string, unknown>)
        : null;
    const candidate = obj.a2ui ?? nested?.a2ui;
    if (
      candidate &&
      typeof candidate === 'object' &&
      'surfaceKind' in candidate &&
      'components' in candidate
    ) {
      return candidate as Surface;
    }
  } catch {
    /* a malformed surface must never break completion */
  }
  return null;
}
