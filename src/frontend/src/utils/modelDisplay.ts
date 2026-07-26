/**
 * Human-readable labels for model keys, guaranteed to stay distinguishable.
 *
 * The label ladder used to be inlined (twice) in the chat model picker, and its
 * catch-all `includes('gpt-5') -> 'GPT-5'` rendered gpt-5.6-sol, gpt-5.6-terra
 * and gpt-5.6-luna as three identical "GPT-5" entries — you could not tell which
 * one you were selecting. Prettifying is only worth doing if it stays lossless,
 * so `buildModelLabels` falls back to raw names for any group that collides.
 */

/** Irregular ids worth naming by hand; matched as substrings, longest first. */
const SPECIAL_CASES: ReadonlyArray<readonly [string, string]> = [
  ['gpt-oss-120b', 'GPT OSS 120B'],
  ['gpt-oss-20b', 'GPT OSS 20B'],
  ['llama-4-maverick', 'Llama 4'],
  ['meta-llama-3-1-405b', 'Llama 3.1 405B'],
  ['meta-llama-3-3-70b', 'Llama 3.3 70B'],
  ['gemini-2-5-pro', 'Gemini 2.5 Pro'],
  ['gemini-2-5-flash', 'Gemini 2.5 Flash'],
  ['claude-sonnet-4-5', 'Claude Sonnet 4.5'],
  ['claude-3-7-sonnet', 'Claude 3.7 Sonnet'],
  ['claude-sonnet-4', 'Claude Sonnet 4'],
  ['qwen3-next-80b', 'Qwen3 Next 80B'],
  ['gemma-3-12b', 'Gemma 3 12B'],
];

/**
 * GPT-5 family: keep BOTH the version and the variant, which is exactly what the
 * old ladder threw away. "gpt-5.6-sol" -> "GPT-5.6 Sol", "gpt-5-nano" ->
 * "GPT-5 Nano", "gpt-5-3-codex" -> "GPT-5.3 Codex".
 */
const GPT5_RE = /^gpt-5(?:[.-](\d+))?(?:-(.+))?$/;

function titleCase(token: string): string {
  return token
    .split('-')
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(' ');
}

/** Label for a single model name. Never returns an empty string. */
export function formatModelLabel(name: string): string {
  if (!name) return '';
  // Provider prefix carries no information in a list already grouped by provider.
  const base = name.replace(/^databricks-/, '');

  for (const [needle, label] of SPECIAL_CASES) {
    if (base.includes(needle)) return label;
  }

  const gpt5 = base.match(GPT5_RE);
  if (gpt5) {
    const [, minor, variant] = gpt5;
    const version = minor ? `GPT-5.${minor}` : 'GPT-5';
    return variant ? `${version} ${titleCase(variant)}` : version;
  }

  return base;
}

/**
 * Labels for a whole picker, keyed by model key.
 *
 * Any label claimed by more than one key is discarded for that group and the raw
 * name is used instead — a slightly uglier list beats one where two rows read
 * the same. Callers should use this rather than mapping `formatModelLabel`
 * themselves, so the guarantee actually holds.
 */
export function buildModelLabels(
  models: Record<string, { name?: string } | undefined>
): Record<string, string> {
  const raw: Record<string, string> = {};
  const counts: Record<string, number> = {};

  for (const [key, model] of Object.entries(models)) {
    const name = model?.name || key;
    const label = formatModelLabel(name) || name;
    raw[key] = label;
    counts[label] = (counts[label] || 0) + 1;
  }

  const labels: Record<string, string> = {};
  for (const [key, label] of Object.entries(raw)) {
    labels[key] = counts[label] > 1 ? models[key]?.name || key : label;
  }
  return labels;
}
