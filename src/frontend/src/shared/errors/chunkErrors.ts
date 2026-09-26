/**
 * Recognising a failed lazy chunk.
 *
 * After a redeploy the chunk names change (they carry a content hash). A tab
 * opened before the deploy still asks for the OLD names, the server has none
 * of them, and the dynamic import rejects. Reloading fetches the new index and
 * fixes it, so the UI should offer exactly that rather than a generic error.
 *
 * Browsers word the rejection differently, and Vite adds its own for CSS
 * preloads, so this matches on the known phrasings.
 */
const CHUNK_ERROR_PATTERNS: RegExp[] = [
  /Failed to fetch dynamically imported module/i, // Chromium
  /error loading dynamically imported module/i, // Firefox
  /Importing a module script failed/i, // Safari
  /Unable to preload CSS/i, // Vite CSS preload
  /Loading (CSS )?chunk [\w-]+ failed/i, // webpack-style wording
];

export function isChunkLoadError(error: unknown): boolean {
  if (!error) return false;
  const name = (error as { name?: unknown }).name;
  if (name === 'ChunkLoadError') return true;
  const message =
    typeof error === 'string' ? error : String((error as { message?: unknown }).message ?? '');
  return CHUNK_ERROR_PATTERNS.some((pattern) => pattern.test(message));
}

/** Reload the page; a seam so tests can observe it without navigating. */
export function reloadPage(): void {
  window.location.reload();
}
