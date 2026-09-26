/** Endpoint rules for Configuration → Models (see services/llm/endpoints.py). */

/** Providers served from a machine you run: the endpoint is required in Apps. */
export const SELF_HOSTED_PROVIDERS = ['vllm', 'ollama', 'custom'];
/** Hosted providers whose public API can be overridden (e.g. a gateway). */
export const HOSTED_OVERRIDABLE = ['anthropic', 'deepseek', 'gemini', 'kimi'];

export const LOCAL_DEFAULTS: Record<string, string> = {
  vllm: 'http://localhost:8081/v1',
  ollama: 'http://localhost:11434',
  custom: 'http://127.0.0.1:8082/v1',
};

export type ModelParams = Record<string, unknown>;

/** A valid http(s) URL, or empty (use the default). */
export function endpointError(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  try {
    const url = new URL(trimmed);
    return url.protocol === 'http:' || url.protocol === 'https:' ? null : 'Use an http:// or https:// URL.';
  } catch {
    return 'Enter a full URL, e.g. https://vllm.example.com/v1';
  }
}
