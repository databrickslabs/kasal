// Durable marker for an in-flight crew run, so a page refresh can reconnect.
//
// The execution store is in-memory and wiped on reload — without this the chat
// loses the running state (and the Stop button) even though the backend job is
// still going. We persist the running job per session in IndexedDB (the same
// store the chat uses for messages/preview, which reliably survives a refresh;
// localStorage proved flaky in this environment — markers written during a run
// were absent after reload despite no removal).
//
// Kept in its own module (not the execution store) so components can read/clear
// it without depending on the store, and so unit tests that mock the store
// still get the real helpers.

import {
  setSessionRunningJob,
  getSessionRunningJob,
  clearSessionRunningJob,
} from '../db/sessionDb';

/** Record the in-flight job for a session (fire-and-forget; safe from sync callers). */
export function persistActiveExecution(sessionId: string, jobId: string): void {
  void setSessionRunningJob(sessionId, jobId).catch(() => {
    /* IndexedDB unavailable — reconnect just won't happen, not fatal */
  });
}

/** Read the in-flight job for a session, or null. Async (IndexedDB). */
export async function readActiveExecution(sessionId: string): Promise<string | null> {
  try {
    return await getSessionRunningJob(sessionId);
  } catch {
    return null;
  }
}

/** Clear the in-flight-job marker for a session (run finished/stopped/failed). */
export function clearActiveExecution(sessionId: string): void {
  void clearSessionRunningJob(sessionId).catch(() => {
    /* ignore */
  });
}
