/**
 * What a model selector falls back to when its model is no longer enabled.
 *
 * One rule for every surface, so the chat composer and the builders never
 * disagree: the server default when it is enabled, else the first enabled
 * model, else nothing. (The chat adds Auto in front of this when the decision
 * model is available — see `resolveChatModel` in features/chat/utils/autoModel.)
 */
export function fallbackModelKey(models: { key: string }[], serverDefault: string): string {
  if (models.length === 0) return '';
  return models.some((m) => m.key === serverDefault) ? serverDefault : models[0].key;
}

/**
 * The builder assistant's model given the live enabled list.
 *
 * An enabled model is kept exactly as chosen. A model that is not (disabled,
 * deleted, or the bootstrap default this deployment never enabled) falls back.
 * An empty list means "not known yet" and changes nothing.
 */
export function resolveBuilderModel(
  current: string,
  models: { key: string }[],
  serverDefault: string,
): string {
  if (models.length === 0) return current;
  if (current && models.some((m) => m.key === current)) return current;
  return fallbackModelKey(models, serverDefault);
}
