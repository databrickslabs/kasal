import { vi, Mock, beforeEach, afterEach, describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useKnowledgeConfigStore } from './knowledgeConfigStore';
import { MemoryBackendService } from '../api/MemoryBackendService';
import { DatabricksService } from '../api/DatabricksService';
import { MemoryBackendType } from '../types/memoryBackend';

// Mock the API services so the store never hits the network.
vi.mock('../api/MemoryBackendService');
vi.mock('../api/DatabricksService');

// Snapshot of the pristine initial state so we can fully reset between tests.
const INITIAL_STATE = {
  isMemoryBackendConfigured: false,
  isKnowledgeSourceEnabled: false,
  isLoading: false,
  lastChecked: 0,
  hasCheckedOnce: false,
  lastNotFoundTime: 0,
};

const resetStore = () => {
  act(() => {
    useKnowledgeConfigStore.setState({ ...INITIAL_STATE });
  });
};

describe('knowledgeConfigStore', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetStore();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('initial state', () => {
    it('has the expected default values', () => {
      const { result } = renderHook(() => useKnowledgeConfigStore());

      expect(result.current.isMemoryBackendConfigured).toBe(false);
      expect(result.current.isKnowledgeSourceEnabled).toBe(false);
      expect(result.current.isLoading).toBe(false);
      expect(result.current.lastChecked).toBe(0);
      expect(result.current.hasCheckedOnce).toBe(false);
      expect(result.current.lastNotFoundTime).toBe(0);
    });
  });

  describe('setMemoryBackendConfigured', () => {
    it('sets the flag to true', () => {
      const { result } = renderHook(() => useKnowledgeConfigStore());
      act(() => {
        result.current.setMemoryBackendConfigured(true);
      });
      expect(result.current.isMemoryBackendConfigured).toBe(true);
    });

    it('sets the flag to false', () => {
      const { result } = renderHook(() => useKnowledgeConfigStore());
      act(() => {
        result.current.setMemoryBackendConfigured(true);
      });
      act(() => {
        result.current.setMemoryBackendConfigured(false);
      });
      expect(result.current.isMemoryBackendConfigured).toBe(false);
    });
  });

  describe('setKnowledgeSourceEnabled', () => {
    it('sets the flag to true', () => {
      const { result } = renderHook(() => useKnowledgeConfigStore());
      act(() => {
        result.current.setKnowledgeSourceEnabled(true);
      });
      expect(result.current.isKnowledgeSourceEnabled).toBe(true);
    });

    it('sets the flag to false', () => {
      const { result } = renderHook(() => useKnowledgeConfigStore());
      act(() => {
        result.current.setKnowledgeSourceEnabled(true);
      });
      act(() => {
        result.current.setKnowledgeSourceEnabled(false);
      });
      expect(result.current.isKnowledgeSourceEnabled).toBe(false);
    });
  });

  describe('checkConfiguration', () => {
    it('marks memory configured when lakebase memory config is complete', async () => {
      (MemoryBackendService.getConfig as Mock).mockResolvedValue({
        backend_type: MemoryBackendType.LAKEBASE,
        lakebase_config: {
          memory_table: 'crew_memory',
          embedding_dimension: 1024,
        },
      });
      (DatabricksService.getConfiguration as Mock).mockResolvedValue({
        knowledge_volume_enabled: true,
        knowledge_volume_path: '/Volumes/x',
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(result.current.isMemoryBackendConfigured).toBe(true);
      expect(result.current.isKnowledgeSourceEnabled).toBe(true);
      expect(result.current.isLoading).toBe(false);
      expect(result.current.hasCheckedOnce).toBe(true);
      expect(result.current.lastNotFoundTime).toBe(0);
    });

    it('marks memory NOT configured for a databricks (Vector Search) backend', async () => {
      // Vector Search has been removed; knowledge embeddings need Lakebase pgvector.
      (MemoryBackendService.getConfig as Mock).mockResolvedValue({
        backend_type: MemoryBackendType.DATABRICKS,
        databricks_config: {
          endpoint_name: 'ep',
          memory_index: 'cat.schema.mem',
        },
      });
      (DatabricksService.getConfiguration as Mock).mockResolvedValue({
        knowledge_volume_enabled: true,
        knowledge_volume_path: '/Volumes/x',
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(result.current.isMemoryBackendConfigured).toBe(false);
    });

    it('marks memory NOT configured when backend type is default', async () => {
      (MemoryBackendService.getConfig as Mock).mockResolvedValue({
        backend_type: MemoryBackendType.DEFAULT,
        databricks_config: {
          endpoint_name: 'ep',
          memory_index: 'cat.schema.mem',
        },
      });
      (DatabricksService.getConfiguration as Mock).mockResolvedValue({
        knowledge_volume_enabled: false,
        knowledge_volume_path: '',
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(result.current.isMemoryBackendConfigured).toBe(false);
      expect(result.current.isKnowledgeSourceEnabled).toBe(false);
      expect(result.current.hasCheckedOnce).toBe(true);
      // databricksConfig was truthy, so this is the "at least one configured" branch
      expect(result.current.lastNotFoundTime).toBe(0);
    });

    it('marks memory NOT configured when databricks_config is incomplete', async () => {
      (MemoryBackendService.getConfig as Mock).mockResolvedValue({
        backend_type: MemoryBackendType.DATABRICKS,
        databricks_config: {
          endpoint_name: 'ep',
          // memory_index missing
        },
      });
      (DatabricksService.getConfiguration as Mock).mockResolvedValue({
        knowledge_volume_enabled: true,
        // knowledge_volume_path missing -> knowledge source not enabled
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(result.current.isMemoryBackendConfigured).toBe(false);
      expect(result.current.isKnowledgeSourceEnabled).toBe(false);
    });

    it('records lastNotFoundTime when both services return null (404s)', async () => {
      (MemoryBackendService.getConfig as Mock).mockResolvedValue(null);
      (DatabricksService.getConfiguration as Mock).mockResolvedValue(null);

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(result.current.isMemoryBackendConfigured).toBe(false);
      expect(result.current.isKnowledgeSourceEnabled).toBe(false);
      expect(result.current.hasCheckedOnce).toBe(true);
      expect(result.current.lastNotFoundTime).toBeGreaterThan(0);
    });

    it('handles exceptions by resetting flags and recording lastNotFoundTime', async () => {
      (MemoryBackendService.getConfig as Mock).mockRejectedValue(new Error('boom'));
      (DatabricksService.getConfiguration as Mock).mockResolvedValue(null);

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(result.current.isMemoryBackendConfigured).toBe(false);
      expect(result.current.isKnowledgeSourceEnabled).toBe(false);
      expect(result.current.isLoading).toBe(false);
      expect(result.current.hasCheckedOnce).toBe(true);
      expect(result.current.lastNotFoundTime).toBeGreaterThan(0);
    });

    it('skips the check when within the 404 cache window', async () => {
      const now = Date.now();
      // Seed a recent 404 so the cache short-circuit triggers.
      act(() => {
        useKnowledgeConfigStore.setState({
          hasCheckedOnce: true,
          lastNotFoundTime: now,
          lastChecked: now,
        });
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(MemoryBackendService.getConfig).not.toHaveBeenCalled();
      expect(DatabricksService.getConfiguration).not.toHaveBeenCalled();
    });

    it('re-checks once the 404 cache window has elapsed', async () => {
      const stale = Date.now() - (5 * 60 * 1000 + 5000); // older than CACHE_DURATION
      act(() => {
        useKnowledgeConfigStore.setState({
          hasCheckedOnce: true,
          lastNotFoundTime: stale,
          lastChecked: stale,
        });
      });

      (MemoryBackendService.getConfig as Mock).mockResolvedValue(null);
      (DatabricksService.getConfiguration as Mock).mockResolvedValue(null);

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(MemoryBackendService.getConfig).toHaveBeenCalled();
    });

    it('throttles checks that happen within 1 second of the last one', async () => {
      const now = Date.now();
      // Recent lastChecked, not loading, no prior 404 -> hits the throttle guard.
      act(() => {
        useKnowledgeConfigStore.setState({
          lastChecked: now,
          isLoading: false,
          hasCheckedOnce: false,
          lastNotFoundTime: 0,
        });
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      expect(MemoryBackendService.getConfig).not.toHaveBeenCalled();
    });

    it('does NOT throttle when isLoading is true even if recently checked', async () => {
      const now = Date.now();
      (MemoryBackendService.getConfig as Mock).mockResolvedValue(null);
      (DatabricksService.getConfiguration as Mock).mockResolvedValue(null);

      act(() => {
        useKnowledgeConfigStore.setState({
          lastChecked: now,
          isLoading: true,
          hasCheckedOnce: false,
          lastNotFoundTime: 0,
        });
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.checkConfiguration();
      });

      // isLoading true bypasses the throttle short-circuit, so the call proceeds.
      expect(MemoryBackendService.getConfig).toHaveBeenCalled();
    });
  });

  describe('refreshConfiguration', () => {
    it('clears cache fields and forces a fresh check', async () => {
      const now = Date.now();
      (MemoryBackendService.getConfig as Mock).mockResolvedValue({
        backend_type: MemoryBackendType.LAKEBASE,
        lakebase_config: { memory_table: 'crew_memory' },
      });
      (DatabricksService.getConfiguration as Mock).mockResolvedValue(null);

      // Seed a recent 404 that would normally cause checkConfiguration to skip.
      act(() => {
        useKnowledgeConfigStore.setState({
          hasCheckedOnce: true,
          lastNotFoundTime: now,
          lastChecked: now,
        });
      });

      const { result } = renderHook(() => useKnowledgeConfigStore());

      await act(async () => {
        await result.current.refreshConfiguration();
      });

      // Because refresh reset lastChecked & lastNotFoundTime, the check ran.
      expect(MemoryBackendService.getConfig).toHaveBeenCalled();
      expect(result.current.isMemoryBackendConfigured).toBe(true);
    });
  });

  // Module-level window event listeners registered at import time.
  describe('global event listeners', () => {
    it('refreshes configuration on the "databricks-config-updated" event', () => {
      const spy = vi.spyOn(useKnowledgeConfigStore.getState(), 'refreshConfiguration');
      act(() => {
        window.dispatchEvent(new Event('databricks-config-updated'));
      });
      expect(spy).toHaveBeenCalled();
    });

    it('refreshes configuration on the "memory-backend-updated" event', () => {
      const spy = vi.spyOn(useKnowledgeConfigStore.getState(), 'refreshConfiguration');
      act(() => {
        window.dispatchEvent(new Event('memory-backend-updated'));
      });
      expect(spy).toHaveBeenCalled();
    });

    it('re-checks configuration when the window regains focus', () => {
      const spy = vi.spyOn(useKnowledgeConfigStore.getState(), 'checkConfiguration');
      act(() => {
        window.dispatchEvent(new Event('focus'));
      });
      expect(spy).toHaveBeenCalled();
    });
  });
});
