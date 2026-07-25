/**
 * The default LLM model, in ONE place.
 *
 * Before this, the model name was written literally in 23 places across the
 * frontend (agent forms, generation dialogs, node rendering, the manager-node
 * hook, the API service...). Changing the default meant finding every one of
 * them, and any that was missed silently disagreed with the backend — the UI
 * would show one model as "the default" while the server used another.
 *
 * The server is the authority. Every backend default derives from a single
 * constant (`DEFAULT_ENGINE_MODEL`, overridable per deployment via
 * `DEFAULT_LLM_MODEL`), and it ships that value on the models endpoints as
 * `default_model`. `ModelService` records it here as soon as any model list is
 * fetched, so the UI reflects what the server would actually pick.
 *
 * The literal below is a bootstrap value only: it is what the UI shows in the
 * moments before the first models response arrives, or if the API is
 * unreachable. It is deliberately the sole hardcoded model name in the app.
 */

const BOOTSTRAP_DEFAULT_MODEL = 'databricks-claude-sonnet-4-6';

let serverDefaultModel: string | null = null;

/**
 * Record the server's default. Called by ModelService when a models response
 * carries `default_model`; ignores empty values so a malformed response cannot
 * blank the default.
 */
export function setServerDefaultModel(model: string | undefined | null): void {
  if (model && model.trim()) {
    serverDefaultModel = model.trim();
  }
}

/**
 * The model to use when none is chosen — the server's value once known,
 * otherwise the bootstrap literal.
 */
export function getDefaultModel(): string {
  return serverDefaultModel ?? BOOTSTRAP_DEFAULT_MODEL;
}
