import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock the shared Kasal API config so we control apiClient.defaults.baseURL
// and the fallback config.apiUrl without touching real axios/network.
vi.mock('../../../config/api/ApiConfig', () => {
  const apiClient = {
    defaults: {
      baseURL: 'https://example.com/api/v1',
    },
  };
  return {
    __esModule: true,
    default: apiClient,
    apiClient,
    config: {
      apiUrl: 'https://fallback.example.com/api/v1',
    },
  };
});

import { updateClient, getClient, getBaseUrl } from './client';
import apiClient, { config as kasalApiConfig } from '../../../config/api/ApiConfig';

describe('ChatMode api/client', () => {
  beforeEach(() => {
    // Reset mocked config to known defaults between tests.
    apiClient.defaults.baseURL = 'https://example.com/api/v1';
    kasalApiConfig.apiUrl = 'https://fallback.example.com/api/v1';
  });

  describe('updateClient', () => {
    it('is a no-op that returns undefined', () => {
      expect(updateClient({} as never)).toBeUndefined();
    });
  });

  describe('getClient', () => {
    it('returns the shared Kasal apiClient instance', () => {
      expect(getClient()).toBe(apiClient);
    });
  });

  describe('getBaseUrl', () => {
    it('returns apiClient.defaults.baseURL when set', () => {
      apiClient.defaults.baseURL = 'https://example.com/api/v1';
      expect(getBaseUrl()).toBe('https://example.com/api/v1');
    });

    it('falls back to config.apiUrl when baseURL is unset', () => {
      apiClient.defaults.baseURL = undefined;
      kasalApiConfig.apiUrl = 'https://fallback.example.com/api/v1';
      expect(getBaseUrl()).toBe('https://fallback.example.com/api/v1');
    });

    it('returns empty string when both baseURL and config.apiUrl are unset', () => {
      apiClient.defaults.baseURL = undefined;
      (kasalApiConfig as { apiUrl: string }).apiUrl = '';
      expect(getBaseUrl()).toBe('');
    });
  });
});
