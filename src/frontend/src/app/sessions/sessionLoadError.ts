import { isAxiosError } from 'axios';

export type SessionLoadStage = 'list' | 'messages' | 'builders' | 'restore';

export class SessionLoadError extends Error {
  constructor(readonly stage: SessionLoadStage, readonly cause: unknown) {
    super(cause instanceof Error ? cause.message : 'Session restore failed');
    this.name = 'SessionLoadError';
  }
}

/** Show actionable diagnostics without copying server bodies or chat content. */
export function describeSessionLoadError(error: unknown, fallback: SessionLoadStage): string {
  const stage = error instanceof SessionLoadError ? error.stage : fallback;
  const cause = error instanceof SessionLoadError ? error.cause : error;
  const status = isAxiosError(cause) ? cause.response?.status : undefined;
  const label = { list: 'Session list', messages: 'The selected conversation', builders: 'Builder canvases', restore: 'Session state' }[stage];
  let detail = `${label} could not be restored. Retry to try again.`;
  if (status === 401) detail = `${label} could not be restored. Sign in again, then retry.`;
  else if (status === 403) detail = `${label} could not be restored. Check your access to the selected workspace.`;
  else if (status === 404) detail = `${label} could not be found. Select another session or retry.`;
  else if (stage === 'builders' && (status === 409 || (cause instanceof Error
    && cause.message === 'This session changed in another browser. Your unsaved canvas is retained locally.'))) {
    detail = 'A builder canvas changed in another browser. Your unsaved canvas is retained locally. Chat sessions are still available.';
  } else if (status && status >= 500) detail = `${label} could not be restored because the server failed. Retry, or share the diagnostic below.`;
  else if (isAxiosError(cause) && !cause.response) detail = `${label} could not be restored because the server could not be reached. Check your connection and retry.`;
  const diagnostic = `HISTORY-v1/${stage}/HTTP-${status ?? 'unavailable'}`;
  console.warn('Session restore failed:', diagnostic);
  return `${detail} Diagnostic: ${diagnostic}.`;
}
