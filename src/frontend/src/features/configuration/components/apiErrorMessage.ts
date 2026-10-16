/** An Axios-style error as the database management screen receives it. */
export interface ErrorWithResponse {
  response?: {
    status?: number;
    data?: {
      // A string for Kasal's errors; FastAPI's 422 sends a list of issues.
      detail?: unknown;
      error?: unknown;
    };
  };
  message?: string;
}

export function isErrorWithResponse(error: unknown): error is ErrorWithResponse {
  return typeof error === 'object' && error !== null && 'response' in error;
}

/**
 * The server's message for a failed request, or `fallback`.
 *
 * Kasal's domain errors (a 400 such as "export is not available on Lakebase")
 * answer `{"detail": ...}`; some database-management failures answer
 * `{"error": ...}`. Read `detail` first so the reason reaches the user.
 */
export function apiErrorMessage(error: unknown, fallback: string): string {
  if (!isErrorWithResponse(error)) return fallback;
  const { detail, error: message } = error.response?.data ?? {};
  // Only a non-empty string is shown: a 422's list of issues is not text.
  if (typeof detail === 'string' && detail) return detail;
  if (typeof message === 'string' && message) return message;
  return fallback;
}
