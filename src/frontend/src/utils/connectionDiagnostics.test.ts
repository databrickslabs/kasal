import { afterEach, expect, it, vi } from 'vitest';
import { CONNECTION_DIAGNOSTICS_KEY, recordConnectionDiagnostic } from './connectionDiagnostics';

afterEach(() => { vi.restoreAllMocks(); sessionStorage.clear(); });

it('keeps only the last 50 connection events across module reloads', async () => {
  for (let attempt = 0; attempt < 60; attempt++) recordConnectionDiagnostic({ kind: 'sse-error', attempt });
  vi.resetModules();
  const reloaded = await import('./connectionDiagnostics');
  reloaded.recordConnectionDiagnostic({ kind: 'sse-open' });
  const history = JSON.parse(sessionStorage.getItem(CONNECTION_DIAGNOSTICS_KEY)!);
  expect(history).toHaveLength(50);
  expect(history[0].attempt).toBe(11);
  expect(history[49]).toEqual({ kind: 'sse-open', at: expect.any(String) });
});

it('does not interrupt recovery if browser storage is unavailable', () => {
  vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('blocked'); });
  expect(() => recordConnectionDiagnostic({ kind: 'poll-timeout' })).not.toThrow();
});
