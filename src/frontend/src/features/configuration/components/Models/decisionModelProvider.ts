/**
 * How the deployment reaches its decision model: the native Jev API or
 * OpenRouter. The choice is the `decision_connection` system setting; each
 * connection has its own URL setting and its own per-workspace API key.
 *
 * `apiKeyName` is the stored API Keys name and must not change: workspaces
 * already saved their keys under these names.
 */
export type DecisionConnectionId = 'jev' | 'openrouter';

export interface DecisionModelConnection {
  id: DecisionConnectionId;
  /** Shown as "Connection: <name>". */
  name: string;
  /** The per-workspace credential in Configuration → API Keys. */
  apiKeyName: string;
  /** The URL field reads "<urlName> API URL". */
  urlName: string;
  placeholder: string;
  /** Whether an empty URL leaves the decision model off (no built-in default). */
  urlRequired: boolean;
}

export const DECISION_CONNECTIONS: Record<DecisionConnectionId, DecisionModelConnection> = {
  jev: {
    id: 'jev',
    name: 'Jev API',
    apiKeyName: 'JEV_API_KEY',
    urlName: 'Jev',
    placeholder: 'https://jev.example.com',
    urlRequired: true,
  },
  openrouter: {
    id: 'openrouter',
    name: 'OpenRouter',
    apiKeyName: 'OPENROUTER_API_KEY',
    urlName: 'OpenRouter',
    placeholder: 'https://openrouter.ai/api/v1',
    urlRequired: false,
  },
};

/** The connection for a stored id; anything unknown is the Jev API (the default). */
export function decisionConnection(id: string | null | undefined): DecisionModelConnection {
  return id === 'openrouter' ? DECISION_CONNECTIONS.openrouter : DECISION_CONNECTIONS.jev;
}
