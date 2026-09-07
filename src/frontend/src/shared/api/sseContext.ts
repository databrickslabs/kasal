/**
 * Native EventSource cannot set the development auth headers used by apiClient.
 * For a direct loopback stream, carry the same non-secret local identity and
 * selected workspace in the URL. The backend accepts these only on loopback SSE
 * requests and ignores them in production/Databricks Apps.
 */
export function withLocalSseContext(url: string): string {
  if (!import.meta.env.DEV || typeof window === 'undefined') return url;

  let parsed: URL;
  try {
    parsed = new URL(url, window.location.origin);
  } catch {
    return url;
  }
  if (!['localhost', '127.0.0.1', '::1'].includes(parsed.hostname)) return url;

  const email = import.meta.env.VITE_DEV_USER_EMAIL || 'dev@localhost';
  const groupId = window.localStorage?.getItem('selectedGroupId');
  parsed.searchParams.set('_sse_email', email);
  if (groupId) parsed.searchParams.set('_sse_group_id', groupId);
  return parsed.toString();
}
