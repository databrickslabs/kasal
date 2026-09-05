/**
 * `asset:<id>` references in agent-written HTML.
 *
 * The model never sees an image's bytes; it is told the image's name, size and
 * id, and writes `<img src="asset:<id>">`. Before HTML reaches a frame (a deck
 * slide, a diagram) the references are swapped for the real bytes as data
 * URLs. The swap happens at RENDER time only — what is stored, edited and sent
 * back to the model always keeps the reference, so a deck stays small and an
 * image can be replaced later without touching the HTML.
 */

const REF = /asset:([A-Za-z0-9][A-Za-z0-9_-]{5,63})/g;

/** A transparent pixel, shown while an image's bytes are still on their way. */
export const LOADING_PIXEL =
  'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';

/** The distinct asset ids referenced in `html`, in order of first appearance. */
export function findAssetIds(html: string): string[] {
  const ids: string[] = [];
  const seen = new Set<string>();
  for (const m of (html || '').matchAll(REF)) {
    if (!seen.has(m[1])) {
      seen.add(m[1]);
      ids.push(m[1]);
    }
  }
  return ids;
}

/**
 * `html` with each `asset:<id>` replaced by its URL from `urls`, or by the
 * loading pixel when the bytes are not in yet — a broken-image icon is not a
 * loading state.
 */
export function substituteAssetRefs(html: string, urls: Record<string, string>): string {
  if (!html || !html.includes('asset:')) return html || '';
  return html.replace(REF, (whole, id: string) => urls[id] ?? LOADING_PIXEL);
}

/**
 * True while `html` still carries the loading pixel for some image. Consumers
 * key their frame on this: a frame whose `srcdoc` is changed from the pixel
 * version to the real one while its first navigation is still pending keeps
 * the OLD document (Chrome drops the second navigation), which left the deck
 * studio — several frames mounted at once — showing blank slides. A key flip
 * remounts the frame with the final document instead of re-navigating it.
 */
export function hasPendingAssets(html: string): boolean {
  return (html || '').includes(LOADING_PIXEL);
}
