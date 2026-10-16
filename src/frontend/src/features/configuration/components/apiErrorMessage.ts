/** An Axios-style error as the database management screen receives it. */
export interface ErrorWithResponse {
  response?: {
    status?: number;
    data?: {
      detail?: string;
      error?: string;
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
  return error.response?.data?.detail || error.response?.data?.error || fallback;
}
