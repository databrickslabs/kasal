import { vi, describe, it, expect } from 'vitest';
import { AgentBricksService, AgentBricksEndpoint } from './AgentBricksService';

// The module imports `apiClient` at load time; mock it so importing the module
// (and exercising the pure static helpers) needs no real network client.
vi.mock('../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  },
  config: {
    apiUrl: 'http://localhost:8000/api/v1',
  },
}));

describe('AgentBricksService', () => {
  describe('formatEndpointName', () => {
    it('uses display_name with a "(by creator)" suffix when both are present', () => {
      const endpoint: AgentBricksEndpoint = {
        id: 'e1',
        name: 'serving-endpoint-1',
        display_name: 'My Friendly Agent',
        creator: 'alice@example.com',
      };
      expect(AgentBricksService.formatEndpointName(endpoint)).toBe(
        'My Friendly Agent (by alice@example.com)',
      );
    });

    it('falls back to name when display_name is absent (with creator suffix)', () => {
      const endpoint: AgentBricksEndpoint = {
        id: 'e2',
        name: 'serving-endpoint-2',
        creator: 'bob@example.com',
      };
      expect(AgentBricksService.formatEndpointName(endpoint)).toBe(
        'serving-endpoint-2 (by bob@example.com)',
      );
    });

    it('uses display_name without a suffix when no creator is set', () => {
      const endpoint: AgentBricksEndpoint = {
        id: 'e3',
        name: 'serving-endpoint-3',
        display_name: 'Solo Agent',
      };
      expect(AgentBricksService.formatEndpointName(endpoint)).toBe('Solo Agent');
    });

    it('uses name alone when neither display_name nor creator is set', () => {
      const endpoint: AgentBricksEndpoint = {
        id: 'e4',
        name: 'serving-endpoint-4',
      };
      expect(AgentBricksService.formatEndpointName(endpoint)).toBe('serving-endpoint-4');
    });
  });

  describe('isEndpointReady', () => {
    it('returns true when state is READY', () => {
      const endpoint: AgentBricksEndpoint = { id: 'e1', name: 'ep', state: 'READY' };
      expect(AgentBricksService.isEndpointReady(endpoint)).toBe(true);
    });

    it('returns false when state is not READY', () => {
      expect(
        AgentBricksService.isEndpointReady({ id: 'e1', name: 'ep', state: 'NOT_READY' }),
      ).toBe(false);
      // also false when state is undefined
      expect(AgentBricksService.isEndpointReady({ id: 'e2', name: 'ep2' })).toBe(false);
    });
  });
});
