import { describe, it, expect, vi, beforeEach } from 'vitest';

// A callable axios instance (so the interceptor's retry `apiClient(original)`
// works) that also captures the registered interceptor handlers.
const captured: {
  requestOk?: (c: unknown) => unknown;
  responseErr?: (e: unknown) => unknown;
} = {};

const instance: ReturnType<typeof makeInstance> = makeInstance();

function makeInstance() {
  const fn = vi.fn((cfg: unknown) => Promise.resolve({ data: 'retried', config: cfg }));
  return Object.assign(fn, {
    interceptors: {
      request: { use: vi.fn((ok: (c: unknown) => unknown) => { captured.requestOk = ok; }) },
      response: {
        use: vi.fn((_ok: unknown, err: (e: unknown) => unknown) => { captured.responseErr = err; }),
      },
    },
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    defaults: { headers: { common: {} } },
  });
}

vi.mock('axios', () => ({
  default: { create: vi.fn(() => instance), post: vi.fn() },
}));

describe('shared API client', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('exports apiClient with HTTP methods and a config.apiUrl', async () => {
    const { apiClient, config } = await import('./client');
    expect(apiClient).toBeDefined();
    expect(apiClient.get).toBeDefined();
    expect(typeof config.apiUrl).toBe('string');
    expect(config.apiUrl.length).toBeGreaterThan(0);
  });

  it.each(['/users/me', '/groups/my-groups'])('discovers identity without a stale workspace header: %s', async (url) => {
    await import('./client');
    localStorage.setItem('selectedGroupId', 'user_old_email');
    const config = { url, method: 'get', headers: { group_id: 'user_old_email' } };
    captured.requestOk!(config);
    expect(config.headers.group_id).toBeUndefined();
  });

  it('keeps the selected workspace on execution-history requests', async () => {
    await import('./client');
    localStorage.setItem('selectedGroupId', 'user_allocated');
    const config = { url: '/executions', headers: {} as Record<string, string> };
    captured.requestOk!(config);
    expect(config.headers.group_id).toBe('user_allocated');
  });

  it('never replays a denied write in another workspace', async () => {
    await import('./client');
    localStorage.setItem('selectedGroupId', 'stale');
    const error = { response: { status: 403, data: { detail: 'No access to group stale' } },
      config: { url: '/executions', method: 'post', headers: { group_id: 'stale' } } };
    await expect(captured.responseErr!(error)).rejects.toBe(error);
    expect(instance).not.toHaveBeenCalled();
    expect(localStorage.getItem('selectedGroupId')).toBe('stale');
  });

  describe('stale-workspace (group_id) recovery', () => {
    it('does not loop when the retried workspace request is denied again', async () => {
      await import('./client');
      const error = {
        response: { status: 403, data: { detail: 'No access to group stale' } },
        config: { url: '/users/me', headers: {}, _groupAccessRetry: true },
      };
      await expect(captured.responseErr!(error)).rejects.toBe(error);
      expect(instance).not.toHaveBeenCalled();
    });

    it('reports outages to subscribers and preserves the request rejection', async () => {
      await import('./client');
      const { subscribeToApiFailures } = await import('./errors');
      const listener = vi.fn();
      const unsubscribe = subscribeToApiFailures(listener);
      const error = { response: { status: 503 }, config: { url: '/runs' } };
      try {
        await expect(captured.responseErr!(error)).rejects.toBe(error);
        expect(listener).toHaveBeenCalledWith({ status: 503, url: '/runs' });
      } finally {
        unsubscribe();
      }
      await expect(captured.responseErr!(error)).rejects.toBe(error);
      expect(listener).toHaveBeenCalledTimes(1);
    });

    it('clears the stale selectedGroupId and retries without the header on a group 403', async () => {
      await import('./client'); // registers interceptors → captures handlers
      expect(captured.responseErr).toBeTypeOf('function');

      localStorage.setItem('selectedGroupId', 'marketing_53f80242');
      instance.mockClear();

      const error = {
        response: { status: 403, data: { detail: 'Access denied: User does not have access to group marketing_53f80242' } },
        config: { url: '/users/me', headers: { group_id: 'marketing_53f80242' } },
      };

      const result = await captured.responseErr!(error);

      // Stale selection cleared so subsequent requests fall back to personal.
      expect(localStorage.getItem('selectedGroupId')).toBeNull();
      // Retried once, without the group_id header.
      expect(instance).toHaveBeenCalledTimes(1);
      const retriedConfig = instance.mock.calls[0][0] as { headers: Record<string, unknown>; _groupAccessRetry?: boolean };
      expect(retriedConfig.headers.group_id).toBeUndefined();
      expect(retriedConfig._groupAccessRetry).toBe(true);
      expect(result).toMatchObject({ data: 'retried' });
    });

    it('does not retry a non-group 403', async () => {
      await import('./client');
      instance.mockClear();
      localStorage.setItem('selectedGroupId', 'marketing_53f80242');

      const error = {
        response: { status: 403, data: { detail: 'Forbidden' } },
        config: { url: '/x', headers: {} },
      };

      await expect(captured.responseErr!(error)).rejects.toBe(error);
      expect(instance).not.toHaveBeenCalled();
      expect(localStorage.getItem('selectedGroupId')).toBe('marketing_53f80242');
    });
  });
});

it.each(['/chat-history/sessions/named', '/chat-history/sessions/s/canvas', '/chat-history/sessions/s/messages'])(
  'does not change workspace or replay denied history reads: %s', async (url) => {
    await import('./client');
    instance.mockClear();
    localStorage.setItem('selectedGroupId', 'team');
    const error = { response: { status: 403, data: { detail: 'No access to group team' } },
      config: { url, method: 'get', headers: { group_id: 'team' } } };
    await expect(captured.responseErr!(error)).rejects.toBe(error);
    expect(instance).not.toHaveBeenCalled();
    expect(localStorage.getItem('selectedGroupId')).toBe('team');
  },
);
