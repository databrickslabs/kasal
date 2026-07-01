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

vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }));

describe('ApiConfig', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('exports apiClient with HTTP methods and a config.apiUrl', async () => {
    const { apiClient, config } = await import('./ApiConfig');
    expect(apiClient).toBeDefined();
    expect(apiClient.get).toBeDefined();
    expect(typeof config.apiUrl).toBe('string');
    expect(config.apiUrl.length).toBeGreaterThan(0);
  });

  describe('stale-workspace (group_id) recovery', () => {
    it('clears the stale selectedGroupId and retries without the header on a group 403', async () => {
      await import('./ApiConfig'); // registers interceptors → captures handlers
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
      await import('./ApiConfig');
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
