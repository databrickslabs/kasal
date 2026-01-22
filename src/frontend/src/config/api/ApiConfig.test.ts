import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock axios before importing ApiConfig
vi.mock('axios', () => ({
  default: {
    create: vi.fn(() => ({
      interceptors: {
        request: { use: vi.fn() },
        response: { use: vi.fn() },
      },
      get: vi.fn(),
      post: vi.fn(),
      put: vi.fn(),
      delete: vi.fn(),
      defaults: {
        headers: {
          common: {},
        },
      },
    })),
    post: vi.fn(),
  },
}));

describe('ApiConfig', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports apiClient', async () => {
    const { apiClient } = await import('./ApiConfig');
    expect(apiClient).toBeDefined();
  });

  it('apiClient has HTTP methods', async () => {
    const { apiClient } = await import('./ApiConfig');
    expect(apiClient.get).toBeDefined();
    expect(apiClient.post).toBeDefined();
    expect(apiClient.put).toBeDefined();
    expect(apiClient.delete).toBeDefined();
  });

  it('exports config object with apiUrl', async () => {
    const { config } = await import('./ApiConfig');
    expect(config).toBeDefined();
    expect(typeof config.apiUrl).toBe('string');
  });

  it('config.apiUrl has a default value', async () => {
    const { config } = await import('./ApiConfig');
    // In test environment, it should have some value
    expect(config.apiUrl.length).toBeGreaterThan(0);
  });
});
