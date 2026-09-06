/**
 * Per-run transforms applied to a run's final text before it is posted.
 *
 * Some runs answer with a PIECE of what the reader should see: a slide refine
 * returns one `<section>`, and the deck it belongs in lives only in the chat.
 * The run that started it registers how to turn the piece back into the whole
 * — keyed by job id, applied exactly once at completion, dropped on failure.
 *
 * Module-level, like the execution store's own job maps: a function cannot
 * live in persisted state, and a page reload mid-run simply posts the raw
 * answer (one fenced slide), which still renders.
 */
const transforms = new Map<string, (text: string) => string>();

export function registerResultTransform(jobId: string, transform: (text: string) => string): void {
  if (jobId) transforms.set(jobId, transform);
}

/** The transformed text for this job (the transform is consumed), else the text as is. */
export function applyResultTransform(jobId: string | undefined, text: string): string {
  if (!jobId) return text;
  const fn = transforms.get(jobId);
  if (!fn) return text;
  transforms.delete(jobId);
  try {
    return fn(text);
  } catch {
    // A transform that throws must not cost the reader the answer.
    return text;
  }
}

export function dropResultTransform(jobId: string | undefined): void {
  if (jobId) transforms.delete(jobId);
}
