/** Small, reload-surviving transport history. Never store payloads or credentials. */
export const CONNECTION_DIAGNOSTICS_KEY = 'kasal.connection.diagnostics';

type ConnectionDiagnostic = {
  kind: 'sse-open' | 'sse-error' | 'poll-timeout';
  readyState?: number;
  attempt?: number;
  fatal?: boolean;
};

export function recordConnectionDiagnostic(event: ConnectionDiagnostic): void {
  try {
    const saved = JSON.parse(sessionStorage.getItem(CONNECTION_DIAGNOSTICS_KEY) || '[]');
    const history = Array.isArray(saved) ? saved.slice(-49) : [];
    history.push({ at: new Date().toISOString(), ...event });
    sessionStorage.setItem(CONNECTION_DIAGNOSTICS_KEY, JSON.stringify(history));
  } catch {
    // Restricted storage must never interfere with connection recovery.
  }
}
