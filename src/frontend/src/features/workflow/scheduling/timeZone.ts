/**
 * The timezone a schedule's times are picked and read in.
 *
 * Every schedule dialog picks an hour on the user's own clock, so it sends the
 * browser's IANA zone with the cron line and the server reads the line on that
 * clock. A schedule saved before schedules had a zone runs on the server's
 * clock (UTC on Databricks Apps), and says so.
 */

/** The browser's IANA timezone ("Europe/Berlin"), or undefined if unknown. */
export function browserTimeZone(): string | undefined {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || undefined;
  } catch {
    return undefined;
  }
}

/** How to name a schedule's clock to the user. */
export function timeZoneLabel(timeZone?: string | null): string {
  return timeZone || 'server time (UTC)';
}

/** "Times are in Europe/Berlin" — the line each dialog shows under the time. */
export function timeZoneNote(timeZone?: string | null): string {
  return `Times are in ${timeZoneLabel(timeZone)}`;
}

/** A stored (naive UTC) schedule time, shown in the user's locale and zone. */
export function formatScheduleTime(naiveUtc: string): string {
  return new Date(`${naiveUtc}Z`).toLocaleString(undefined, { timeZoneName: 'short' });
}

/** The server's reason for refusing a request, when it gave one. */
export function refusalDetail(error: unknown, fallback: string): string {
  const detail = (error as { response?: { data?: { detail?: unknown } } } | null)
    ?.response?.data?.detail;
  return typeof detail === 'string' && detail ? detail : fallback;
}
