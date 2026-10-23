/**
 * How the deployment reaches its decision model: TypeSafe's own API,
 * OpenRouter, or Databricks (Jev on the workspace's own AI Gateway). The
 * choice is the `decision_connection` system setting. TypeSafe and
 * OpenRouter each have their own URL setting and their own per-workspace API
 * key; Databricks has neither: it uses each workspace's Databricks credentials,
 * and its model is the `databricks_decision_model` setting.
 *
 * `apiKeyName` is the stored API Keys name and must not change: workspaces
 * already saved their keys under these names.
 */
export type DecisionConnectionId = 'jev' | 'openrouter' | 'databricks';

export interface DecisionModelConnection {
  id: DecisionConnectionId;
  /** Shown as "Connection: <name>". */
  name: string;
  /** The per-workspace credential in Configuration → API Keys; null when there is none (Databricks). */
  apiKeyName: string | null;
  /** The URL field reads "<urlName> API URL"; null when the connection has no URL setting. */
  urlName: string | null;
  placeholder: string;
  /** Whether an empty URL leaves the decision model off (no built-in default). */
  urlRequired: boolean;
  /** Whether every decision is served, not only Auto (OpenRouter serves Auto only). */
  allPolicies: boolean;
}

export const DECISION_CONNECTIONS: Record<DecisionConnectionId, DecisionModelConnection> = {
  jev: {
    id: 'jev',
    name: 'TypeSafe',
    apiKeyName: 'JEV_API_KEY',
    urlName: 'TypeSafe',
    placeholder: 'https://typesafe.example.com',
    urlRequired: true,
    allPolicies: true,
  },
  openrouter: {
    id: 'openrouter',
    name: 'OpenRouter',
    apiKeyName: 'OPENROUTER_API_KEY',
    urlName: 'OpenRouter',
    placeholder: 'https://openrouter.ai/api/v1',
    urlRequired: false,
    allPolicies: false,
  },
  databricks: {
    id: 'databricks',
    name: 'Databricks',
    apiKeyName: null,
    urlName: null,
    placeholder: '',
    urlRequired: false,
    allPolicies: true,
  },
};

/**
 * The Databricks decision models offered by name. The first is the default the
 * backend uses when none is saved (`connection.DATABRICKS_DEFAULT_MODEL`); any
 * other model id can still be typed.
 */
export const DATABRICKS_DECISION_MODELS = [
  { id: 'system.ai.openjev-qwen35-4b', description: 'Databricks-hosted Jev (default)' },
] as const;

export const DATABRICKS_DEFAULT_DECISION_MODEL: string = DATABRICKS_DECISION_MODELS[0].id;

/** The connection for a stored id; anything unknown is TypeSafe (the default). */
export function decisionConnection(id: string | null | undefined): DecisionModelConnection {
  if (id === 'openrouter' || id === 'databricks') return DECISION_CONNECTIONS[id];
  return DECISION_CONNECTIONS.jev;
}
