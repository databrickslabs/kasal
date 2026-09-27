/**
 * The provider behind the "Decision model" setting. Jev is the only one today;
 * a second provider becomes a second entry here plus a selector in the cards.
 *
 * `apiKeyName` is the stored API Keys name and must not change: existing
 * workspaces already saved their key under it. The provider's endpoint is the
 * `jev_api_base` system setting (see DecisionModelSystemSettings).
 */
export interface DecisionModelProvider {
  /** Shown as "Provider: <name>". */
  name: string;
  /** The per-workspace credential in Configuration → API Keys. */
  apiKeyName: string;
}

export const DECISION_MODEL_PROVIDER: DecisionModelProvider = {
  name: 'Jev',
  apiKeyName: 'JEV_API_KEY',
};
