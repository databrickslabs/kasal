import { vi } from 'vitest';

// Create a mock apiClient with all axios methods
export const mockApiClient = {
  get: vi.fn(),
  post: vi.fn(),
  put: vi.fn(),
  delete: vi.fn(),
  patch: vi.fn(),
  request: vi.fn(),
  interceptors: {
    request: { use: vi.fn(), eject: vi.fn() },
    response: { use: vi.fn(), eject: vi.fn() },
  },
  defaults: {
    headers: {
      common: {},
    },
  },
};

export const mockConfig = {
  apiUrl: 'http://localhost:8000/api/v1',
};

// Helper to reset all mocks
export const resetApiMocks = () => {
  mockApiClient.get.mockReset();
  mockApiClient.post.mockReset();
  mockApiClient.put.mockReset();
  mockApiClient.delete.mockReset();
  mockApiClient.patch.mockReset();
  mockApiClient.request.mockReset();
};
