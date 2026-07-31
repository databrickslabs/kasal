import { vi, beforeEach, afterEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor, act, cleanup } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { AxiosError } from 'axios';
import { EMBEDDING_MODELS } from './constants';
import { MemoryBackendType } from '../../types/config/memoryBackend';

// ---------------------------------------------------------------------------
// Hoisted mock references
// ---------------------------------------------------------------------------

const {
  mockApiClient,
  mockUpdateConfig,
  mockMBService,
  mockValidateIndex,
  capturedProps,
} = vi.hoisted(() => ({
  mockApiClient: {
    get: vi.fn().mockResolvedValue({ data: {} }),
    put: vi.fn().mockResolvedValue({ data: {} }),
    post: vi.fn().mockResolvedValue({ data: {} }),
    delete: vi.fn().mockResolvedValue({ data: {} }),
  },
  mockUpdateConfig: vi.fn(),
  mockMBService: {
    switchToDisabledMode: vi.fn().mockResolvedValue({ id: '1', backend_type: 'default' }),
    getLakebaseTableStats: vi.fn().mockResolvedValue({}),
    testLakebaseConnection: vi.fn().mockResolvedValue({ success: true, message: 'Connected' }),
    initializeLakebaseTables: vi.fn().mockResolvedValue({ success: true, message: 'Tables created' }),
    saveDefaultConfig: vi.fn().mockResolvedValue({ success: true, backend_id: 'b1', message: 'ok' }),
  },
  mockValidateIndex: vi.fn().mockReturnValue(true),
  capturedProps: { current: {} as Record<string, Record<string, unknown>> },
}));

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: mockApiClient,
}));

vi.mock('../../store/memoryBackend', () => ({
  // Support both the no-arg destructure (MemoryConfiguration) and the
  // selector form used by MemoryTuningPanel (state => state.updateCognitiveConfig).
  useMemoryBackendStore: (selector?: (state: unknown) => unknown) => {
    const state = {
      updateConfig: mockUpdateConfig,
      updateCognitiveConfig: vi.fn(),
      config: { cognitive_config: {} },
    };
    return selector ? selector(state) : state;
  },
  useMemoryTuningConfig: () => ({}),
}));


vi.mock('../../api/memory/MemoryBackendService', () => ({
  MemoryBackendService: mockMBService,
}));

// MemoryTuningPanel (rendered inside this component) fetches enabled models;
// stub it so the model dropdown resolves deterministically without real API churn.
vi.mock('../../api/config/ModelService', () => ({
  ModelService: {
    getInstance: () => ({
      getActiveModels: vi.fn().mockResolvedValue({}),
      getActiveModelsSync: vi.fn().mockReturnValue({}),
    }),
  },
}));


// ---------------------------------------------------------------------------
// Child component mocks – capture props for callback testing
// ---------------------------------------------------------------------------









vi.mock('./MemoryRecordsBrowser', () => ({
  MemoryRecordsBrowser: (props: Record<string, unknown>) => {
    capturedProps.current.MemoryRecordsBrowser = props;
    return <div data-testid="memory-records-browser" data-open={String(props.open)} />;
  },
}));

// ---------------------------------------------------------------------------
// Import component
// ---------------------------------------------------------------------------

import { MemoryConfiguration } from './MemoryConfiguration';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const theme = createTheme();

function renderComponent() {
  return render(
    <ThemeProvider theme={theme}>
      <MemoryConfiguration />
    </ThemeProvider>,
  );
}

async function waitForLoaded() {
  await waitFor(() => {
    expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
  });
}

function paginatedInstances(
  items: Array<{ name: string; state: string; type?: string; capacity?: string; read_write_dns?: string }>,
  opts: { page?: number; total_pages?: number; has_more?: boolean } = {},
) {
  return {
    data: {
      items,
      total: items.length,
      page: opts.page ?? 1,
      page_size: 30,
      total_pages: opts.total_pages ?? 1,
      has_more: opts.has_more ?? false,
    },
  };
}

/**
 * Mock responses for a full Databricks config scenario.
 *
 * CrewAI 1.10+ memory: a single `memory_index` replaces the
 * legacy short_term/long_term/entity indexes. The saved config maps
 * `memory_index` → `indexes.unified` and `document_index` → `indexes.document`.
 */
function setupDatabricksMocks(overrides?: { verifyMissing?: string[]; indexInfoError?: boolean }) {
  mockApiClient.get.mockImplementation((url: string) => {
    if (url === '/memory-backend/configs/default') {
      return Promise.resolve({
        data: {
          id: 'db-123',
          backend_type: 'databricks',
          databricks_config: {
            workspace_url: 'https://test.databricks.com',
            catalog: 'ml',
            schema: 'agents',
            endpoint_name: 'mem-ep',
            document_endpoint_name: 'doc-ep',
            memory_index: 'ml.agents.mem',
            document_index: 'ml.agents.doc',
          },
        },
      });
    }
    if (url === '/databricks/environment') {
      return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
    }
    if (url === '/memory-backend/databricks/verify-resources') {
      const missing = overrides?.verifyMissing || [];
      const endpoints: Record<string, unknown> = {};
      if (!missing.includes('mem-ep')) endpoints['mem-ep'] = { name: 'mem-ep', state: 'ONLINE', ready: true };
      if (!missing.includes('doc-ep')) endpoints['doc-ep'] = { name: 'doc-ep', state: 'ONLINE', ready: true };
      const indexes: Record<string, unknown> = {};
      for (const idx of ['ml.agents.mem', 'ml.agents.doc']) {
        if (!missing.includes(idx)) indexes[idx] = { name: idx, status: 'ONLINE' };
      }
      return Promise.resolve({ data: { success: true, resources: { endpoints, indexes } } });
    }
    if (url === '/memory-backend/databricks/index-info') {
      if (overrides?.indexInfoError) {
        return Promise.resolve({ data: { success: false, message: 'Index not found' } });
      }
      return Promise.resolve({
        data: { success: true, doc_count: 10, status: 'ONLINE', ready: true, index_type: 'DELTA_SYNC' },
      });
    }
    if (url.startsWith('/memory-backend/configs/')) {
      return Promise.resolve({ data: { id: 'db-123', databricks_config: { embedding_dimension: 1024 } } });
    }
    if (url === '/database-management/lakebase/instances') {
      return Promise.resolve(paginatedInstances([]));
    }
    return Promise.resolve({ data: {} });
  });
}

function setupLakebaseMocks(opts?: { tablesInitialized?: boolean; withStats?: boolean }) {
  const lakebaseConfig: Record<string, unknown> = {
    instance_name: 'my-instance',
    embedding_dimension: 1024,
    memory_table: 'crew_memory',
    tables_initialized: opts?.tablesInitialized ?? false,
  };
  if (opts?.withStats) {
    // Memory uses a single `crew_memory` table.
    mockMBService.getLakebaseTableStats.mockResolvedValue({
      memory: { table_name: 'crew_memory', exists: true, row_count: 18 },
    });
  }
  mockApiClient.get.mockImplementation((url: string) => {
    if (url === '/memory-backend/configs/default') {
      return Promise.resolve({
        data: { id: 'lb-existing', backend_type: 'lakebase', lakebase_config: lakebaseConfig },
      });
    }
    if (url === '/databricks/environment') {
      return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
    }
    if (url === '/database-management/lakebase/instances') {
      return Promise.resolve(paginatedInstances([
        { name: 'my-instance', state: 'ACTIVE', type: 'provisioned' },
        { name: 'auto-instance', state: 'AVAILABLE', type: 'autoscaling' },
      ]));
    }
    if (url === '/memory-backend/configs') {
      return Promise.resolve({ data: [] });
    }
    return Promise.resolve({ data: {} });
  });
}

function makeAxios404(): AxiosError {
  const err = new AxiosError('Not found', '404', undefined, undefined, {
    status: 404, data: { detail: 'Not found' }, statusText: 'Not Found', headers: {}, config: {} as never,
  } as never);
  return err;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('MemoryConfiguration', () => {
  let confirmSpy: ReturnType<typeof vi.spyOn>;
  let alertSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    // Drain any still-pending async work from the previous test (e.g. a late
    // updateBackendConfiguration PUT whose verify→detect→PUT chain outlives the
    // test) BEFORE clearing mocks, so it can't bleed into this test's call
    // assertions. Several cycles cover the multi-await chain.
    for (let i = 0; i < 5; i++) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    vi.clearAllMocks();
    capturedProps.current = {};
    confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);
    alertSpy = vi.spyOn(window, 'alert').mockImplementation(() => {});

    // Default mocks: no existing config
    mockApiClient.get.mockImplementation((url: string) => {
      if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
      if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
      if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
      if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
      return Promise.resolve({ data: {} });
    });
  });

  afterEach(() => {
    // Unmount components between tests so a prior test's still-mounted component
    // can't react to the next test's mock changes (was leaking a stray PUT).
    cleanup();
    vi.restoreAllMocks();
  });

  // =======================================================================
  // Rendering & Loading
  // =======================================================================

  it('renders the Memory Configuration heading', async () => {
    await act(async () => renderComponent());
    expect(screen.getAllByText('Memory Configuration').length).toBeGreaterThanOrEqual(1);
  });

  it('shows loading state initially before config check completes', async () => {
    let resolve!: (v: { data: Record<string, unknown> }) => void;
    mockApiClient.get.mockImplementation((url: string) => {
      if (url === '/memory-backend/configs/default') return new Promise(r => { resolve = r; });
      if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
      return Promise.resolve({ data: {} });
    });

    await act(async () => renderComponent());
    expect(screen.getByText('Loading memory configuration...')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toBeInTheDocument();
    await act(async () => resolve({ data: {} }));
  });

  it('shows Lakebase and Local radio options after loading', async () => {
    await act(async () => renderComponent());
    await waitForLoaded();
    const radios = screen.getAllByRole('radio');
    expect(radios.length).toBe(2);
    expect(screen.getByText('Lakebase (pgvector)')).toBeInTheDocument();
    expect(screen.getByText('Local')).toBeInTheDocument();
    expect(screen.queryByText('Databricks Vector Search')).not.toBeInTheDocument();
  });

  it('shows info alert about local SQLite storage when mode is disabled', async () => {
    await act(async () => renderComponent());
    await waitForLoaded();
    expect(
      screen.getByText(/Kasal memory is stored locally in SQLite/),
    ).toBeInTheDocument();
  });


  it('opens the MemoryRecordsBrowser from the local-mode Browse Memory button', async () => {
    await act(async () => renderComponent());
    await waitForLoaded();

    const browseBtn = screen.getByRole('button', { name: /Browse Memory/i });
    await act(async () => fireEvent.click(browseBtn));

    await waitFor(() => {
      expect(screen.getByTestId('memory-records-browser')).toHaveAttribute('data-open', 'true');
    });
  });

  it('persists local memory tuning via /memory-backend/default/save-config', async () => {
    await act(async () => renderComponent());
    await waitForLoaded();

    const saveBtn = screen.getByRole('button', { name: /Save Local Memory Settings/i });
    await act(async () => fireEvent.click(saveBtn));

    // Local save creates an ACTIVE default config (the fix that makes the
    // memory tuning actually load at crew execution).
    await waitFor(() => {
      expect(mockMBService.saveDefaultConfig).toHaveBeenCalled();
    });
  });

  it('calls apiClient.get for configs/default and databricks/environment on mount', async () => {
    await act(async () => renderComponent());
    await waitFor(() => {
      expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/configs/default');
      expect(mockApiClient.get).toHaveBeenCalledWith('/databricks/environment');
    });
  });

  // =======================================================================
  // Config Loading paths
  // =======================================================================

  describe('Config Loading', () => {
    it('falls back to all configs when default is empty, uses first config', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/memory-backend/configs') {
          return Promise.resolve({
            data: [{ id: 'cfg-1', backend_type: 'lakebase', lakebase_config: { instance_name: 'i1', embedding_dimension: 1024 } }],
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/configs');
    });

    it('falls back to all configs on error, returns empty → disabled mode', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/memory-backend/configs') return Promise.reject(new Error('fail'));
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      expect(screen.getByText(/Kasal memory is stored locally in SQLite/)).toBeInTheDocument();
    });

    it('handles 404 error with fallback to all configs that have data', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.reject(makeAxios404());
        if (url === '/memory-backend/configs') {
          return Promise.resolve({
            data: [{ id: 'cfg-1', backend_type: 'default' }],
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
    });

    it('handles 404 error with fallback that also fails', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.reject(makeAxios404());
        if (url === '/memory-backend/configs') return Promise.reject(new Error('fail'));
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
    });

    it('handles non-404 error → disabled mode', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.reject(new Error('Server error'));
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
    });


    it('loads Lakebase config and switches to lakebase mode', async () => {
      setupLakebaseMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(screen.getByLabelText(/Lakebase Instance/i)).toBeInTheDocument();
      });
    });

    it('loads Lakebase config with tables_initialized and fetches table stats', async () => {
      setupLakebaseMocks({ tablesInitialized: true, withStats: true });
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(mockMBService.getLakebaseTableStats).toHaveBeenCalledWith('my-instance');
      });
    });

    it('loads DEFAULT backend_type and shows disabled mode', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({ data: { id: 'def-1', backend_type: 'default' } });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      expect(screen.getByText(/Kasal memory is stored locally in SQLite/)).toBeInTheDocument();
    });

    it('detectWorkspaceUrl handles error gracefully', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.reject(new Error('no env'));
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
    });
  });

  // =======================================================================
  // Mode Switching
  // =======================================================================

  describe('Mode Switching', () => {
    it('calls updateConfig with DEFAULT when switching to local', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();

      // Switch to lakebase first
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));
      // Switch back to local
      await act(async () => fireEvent.click(screen.getByLabelText(/Local/i)));

      await waitFor(() => {
        expect(mockUpdateConfig).toHaveBeenCalledWith({
          backend_type: MemoryBackendType.DEFAULT,
        });
      });
      expect(mockMBService.switchToDisabledMode).toHaveBeenCalled();
    });

    it('shows error when switchToDisabledMode fails', async () => {
      mockMBService.switchToDisabledMode.mockRejectedValueOnce(new Error('fail'));

      await act(async () => renderComponent());
      await waitForLoaded();

      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));
      await act(async () => fireEvent.click(screen.getByLabelText(/Local/i)));

      await waitFor(() => {
        expect(screen.getByText(/Failed to save disabled mode/)).toBeInTheDocument();
      });
    });

    it('switching to lakebase loads instances and resets config', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();

      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/database-management/lakebase/instances',
          expect.objectContaining({ params: expect.objectContaining({ page: 1 }) }),
        );
      });
    });
  });

  // =======================================================================
  // Lakebase Instances
  // =======================================================================

  describe('Lakebase instance Autocomplete', () => {
    it('shows searchable Autocomplete when Lakebase mode is selected', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));
      await waitFor(() => expect(screen.getByLabelText(/Lakebase Instance/i)).toBeInTheDocument());
    });

    it('fetches paginated instances from the API on open', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') {
          return Promise.resolve(paginatedInstances([
            { name: 'kasal-prod', state: 'AVAILABLE', type: 'autoscaling' },
            { name: 'kasal-dev', state: 'ACTIVE', type: 'provisioned' },
          ]));
        }
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      await act(async () => { fireEvent.focus(input); fireEvent.mouseDown(input); });

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/database-management/lakebase/instances',
          expect.objectContaining({ params: expect.objectContaining({ page: 1, page_size: 30 }) }),
        );
      });
    });

    it('calls loadLakebaseInstances with search param on input change', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      await act(async () => fireEvent.change(input, { target: { value: 'prod' } }));
      await act(async () => vi.advanceTimersByTime(350));

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/database-management/lakebase/instances',
          expect.objectContaining({ params: expect.objectContaining({ search: 'prod', page: 1 }) }),
        );
      });
      vi.useRealTimers();
    });

    it('shows both provisioned and autoscaling instances with type chips', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') {
          return Promise.resolve(paginatedInstances([
            { name: 'kasal-prod', state: 'AVAILABLE', type: 'autoscaling' },
            { name: 'kasal-dev', state: 'ACTIVE', type: 'provisioned' },
          ]));
        }
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      await act(async () => { fireEvent.focus(input); fireEvent.mouseDown(input); });

      await waitFor(() => {
        expect(screen.getByText('kasal-prod')).toBeInTheDocument();
        expect(screen.getByText('kasal-dev')).toBeInTheDocument();
      });
      expect(screen.getByText('Auto')).toBeInTheDocument();
      expect(screen.getByText('Prov')).toBeInTheDocument();
    });

    it('handles instances API error gracefully', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.reject(new Error('network'));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      // Should render without crashing
      expect(screen.getByLabelText(/Lakebase Instance/i)).toBeInTheDocument();
    });

    it('selects an instance from the dropdown', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') {
          return Promise.resolve(paginatedInstances([
            { name: 'my-instance', state: 'ACTIVE', type: 'provisioned' },
          ]));
        }
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      await act(async () => { fireEvent.focus(input); fireEvent.mouseDown(input); });

      await waitFor(() => expect(screen.getByText('my-instance')).toBeInTheDocument());
      await act(async () => fireEvent.click(screen.getByText('my-instance')));
    });

    it('loads more instances on scroll when hasMore is true', async () => {
      mockApiClient.get.mockImplementation((url: string, opts?: { params?: Record<string, unknown> }) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') {
          const page = (opts?.params?.page as number) || 1;
          if (page === 1) {
            return Promise.resolve(paginatedInstances(
              [{ name: 'inst-1', state: 'ACTIVE' }],
              { page: 1, total_pages: 2, has_more: true },
            ));
          }
          return Promise.resolve(paginatedInstances(
            [{ name: 'inst-2', state: 'ACTIVE' }],
            { page: 2, total_pages: 2 },
          ));
        }
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      await act(async () => { fireEvent.focus(input); fireEvent.mouseDown(input); });

      await waitFor(() => expect(screen.getByText('inst-1')).toBeInTheDocument());

      // Simulate scroll to bottom on the listbox
      const listbox = screen.getByRole('listbox');
      Object.defineProperty(listbox, 'scrollTop', { value: 280, writable: true });
      Object.defineProperty(listbox, 'clientHeight', { value: 300, writable: true });
      Object.defineProperty(listbox, 'scrollHeight', { value: 300, writable: true });
      await act(async () => fireEvent.scroll(listbox));

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/database-management/lakebase/instances',
          expect.objectContaining({ params: expect.objectContaining({ page: 2 }) }),
        );
      });
    });

    it('shows helper text when no instances found', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      await waitFor(() => {
        expect(screen.getByText(/No instances found/)).toBeInTheDocument();
      });
    });

    it('renders state chips with correct colors for different states', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') {
          return Promise.resolve(paginatedInstances([
            { name: 'stopped-inst', state: 'STOPPED' },
            { name: 'error-inst', state: 'ERROR' },
            { name: 'unknown-inst', state: 'PENDING' },
          ]));
        }
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      await act(async () => { fireEvent.focus(input); fireEvent.mouseDown(input); });

      await waitFor(() => {
        expect(screen.getByText('STOPPED')).toBeInTheDocument();
        expect(screen.getByText('ERROR')).toBeInTheDocument();
        expect(screen.getByText('PENDING')).toBeInTheDocument();
      });
    });
  });

  // =======================================================================
  // Lakebase UI Interactions
  // =======================================================================

  describe('Lakebase UI', () => {
    it('refreshes instances when refresh button is clicked', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      // The refresh button is an IconButton with RefreshIcon
      const refreshButtons = screen.getAllByRole('button');
      const refreshBtn = refreshButtons.find(b => b.querySelector('[data-testid="RefreshIcon"]'));
      if (refreshBtn) {
        await act(async () => fireEvent.click(refreshBtn));
      }
    });

    it('Test Connection button calls testLakebaseConnection on success', async () => {
      setupLakebaseMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      const testBtn = screen.getByRole('button', { name: /Test Connection/i });
      expect(testBtn).not.toBeDisabled();
      await act(async () => fireEvent.click(testBtn));

      await waitFor(() => {
        expect(mockMBService.testLakebaseConnection).toHaveBeenCalledWith('my-instance');
      });
    });

    it('Test Connection shows error on failure', async () => {
      setupLakebaseMocks();
      mockMBService.testLakebaseConnection.mockRejectedValueOnce(new Error('timeout'));

      await act(async () => renderComponent());
      await waitForLoaded();

      const testBtn = screen.getByRole('button', { name: /Test Connection/i });
      await act(async () => fireEvent.click(testBtn));

      await waitFor(() => {
        expect(screen.getByText('Connection test failed')).toBeInTheDocument();
      });
    });

    it('changes embedding dimension', async () => {
      setupLakebaseMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      const dimInput = screen.getByLabelText(/Embedding Dimension/i);
      await act(async () => fireEvent.change(dimInput, { target: { value: '768' } }));
      expect(dimInput).toHaveValue(768);
    });

    it('Initialize Tables button calls initializeLakebaseTables on success', async () => {
      setupLakebaseMocks();
      mockApiClient.post.mockResolvedValue({ data: { backend_id: 'lb-new' } });

      await act(async () => renderComponent());
      await waitForLoaded();

      const initBtn = screen.getByRole('button', { name: /Initialize Tables/i });
      await act(async () => fireEvent.click(initBtn));

      await waitFor(() => {
        expect(mockMBService.initializeLakebaseTables).toHaveBeenCalledWith(
          expect.objectContaining({ instance_name: 'my-instance' }),
        );
      });
    });

    it('Initialize Tables passes the unified memory_table name', async () => {
      setupLakebaseMocks();
      mockApiClient.post.mockResolvedValue({ data: { backend_id: 'lb-new' } });

      await act(async () => renderComponent());
      await waitForLoaded();

      const initBtn = screen.getByRole('button', { name: /Initialize Tables/i });
      await act(async () => fireEvent.click(initBtn));

      await waitFor(() => {
        expect(mockMBService.initializeLakebaseTables).toHaveBeenCalledWith(
          expect.objectContaining({ memory_table: 'crew_memory' }),
        );
      });
    });

    it('Initialize Tables shows error on failure', async () => {
      setupLakebaseMocks();
      mockMBService.initializeLakebaseTables.mockRejectedValueOnce(new Error('fail'));

      await act(async () => renderComponent());
      await waitForLoaded();

      const initBtn = screen.getByRole('button', { name: /Initialize Tables/i });
      await act(async () => fireEvent.click(initBtn));

      await waitFor(() => {
        expect(screen.getByText('Failed to initialize tables')).toBeInTheDocument();
      });
    });

    it('Initialize Tables saves config to backend after success', async () => {
      setupLakebaseMocks();
      mockMBService.initializeLakebaseTables.mockResolvedValueOnce({ success: true, message: 'Done' });
      mockApiClient.post.mockResolvedValue({ data: { backend_id: 'lb-new' } });

      await act(async () => renderComponent());
      await waitForLoaded();

      const initBtn = screen.getByRole('button', { name: /Initialize Tables/i });
      await act(async () => fireEvent.click(initBtn));

      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith(
          '/memory-backend/lakebase/save-config',
          expect.objectContaining({
            lakebase_config: expect.objectContaining({ tables_initialized: true }),
          }),
        );
      });
    });

    it('Initialize Tables handles save config failure', async () => {
      setupLakebaseMocks();
      mockMBService.initializeLakebaseTables.mockResolvedValueOnce({ success: true, message: 'Done' });
      mockApiClient.post.mockRejectedValueOnce(new Error('save fail'));
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      await act(async () => renderComponent());
      await waitForLoaded();

      const initBtn = screen.getByRole('button', { name: /Initialize Tables/i });
      await act(async () => fireEvent.click(initBtn));

      // The save error is logged
      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledWith('Failed to save Lakebase config:', expect.any(Error));
      });
      consoleSpy.mockRestore();
    });

    it('Refresh Status button reloads table stats', async () => {
      setupLakebaseMocks({ tablesInitialized: true, withStats: true });
      await act(async () => renderComponent());
      await waitForLoaded();

      const refreshBtn = screen.getByRole('button', { name: /Refresh Status/i });
      await act(async () => fireEvent.click(refreshBtn));

      await waitFor(() => {
        // Called once on init and once on refresh
        expect(mockMBService.getLakebaseTableStats).toHaveBeenCalled();
      });
    });

    it('shows Databricks App Setup instructions in lakebase mode', async () => {
      setupLakebaseMocks();
      await act(async () => renderComponent());
      await waitForLoaded();
      expect(screen.getByText(/Databricks App Setup/)).toBeInTheDocument();
    });

    it('opens the MemoryRecordsBrowser from the lakebase Browse Memory button', async () => {
      setupLakebaseMocks({ tablesInitialized: true, withStats: true });
      await act(async () => renderComponent());
      await waitForLoaded();

      // Both the lakebase Memory Tables section and the (collapsed-but-mounted)
      // local-mode alert render a "Browse Memory" button; either opens the
      // shared MemoryRecordsBrowser. Click the first one.
      const browseBtns = await waitFor(() => {
        const btns = screen.getAllByRole('button', { name: /Browse Memory/i });
        expect(btns.length).toBeGreaterThanOrEqual(1);
        return btns;
      });
      await act(async () => fireEvent.click(browseBtns[0]));

      await waitFor(() => {
        expect(screen.getByTestId('memory-records-browser')).toHaveAttribute('data-open', 'true');
      });
    });
  });

  // =======================================================================
  // Lakebase Save Configuration
  // =======================================================================

  describe('Lakebase Save Configuration', () => {
    it('renders Save Configuration button when Lakebase mode is active', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));
      await waitFor(() => expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeInTheDocument());
    });

    it('Save Configuration button is disabled when no instance is selected', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));
      await waitFor(() => expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled());
    });

    it('calls /memory-backend/lakebase/save-config when Save Configuration is clicked', async () => {
      setupLakebaseMocks();
      mockApiClient.post.mockResolvedValue({ data: { backend_id: 'lb-123' } });

      await act(async () => renderComponent());
      await waitForLoaded();

      const saveBtn = await waitFor(() => {
        const btn = screen.getByRole('button', { name: /Save Configuration/i });
        expect(btn).not.toBeDisabled();
        return btn;
      });
      await act(async () => fireEvent.click(saveBtn));

      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith(
          '/memory-backend/lakebase/save-config',
          expect.objectContaining({ lakebase_config: expect.objectContaining({ instance_name: 'my-instance' }) }),
        );
      });
      await waitFor(() => {
        expect(mockUpdateConfig).toHaveBeenCalledWith(
          expect.objectContaining({ backend_type: MemoryBackendType.LAKEBASE }),
        );
      });
    });

    it('shows error status when save-config call fails', async () => {
      setupLakebaseMocks();
      mockApiClient.post.mockRejectedValue(new Error('Network error'));

      await act(async () => renderComponent());
      await waitForLoaded();

      const saveBtn = await waitFor(() => {
        const btn = screen.getByRole('button', { name: /Save Configuration/i });
        expect(btn).not.toBeDisabled();
        return btn;
      });
      await act(async () => fireEvent.click(saveBtn));

      await waitFor(() => {
        expect(screen.getByText(/Failed to save configuration: Network error/)).toBeInTheDocument();
      });
    });

    it('shows success status after successful save', async () => {
      setupLakebaseMocks();
      mockApiClient.post.mockResolvedValue({ data: { backend_id: 'lb-new' } });

      await act(async () => renderComponent());
      await waitForLoaded();

      const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
      await act(async () => fireEvent.click(saveBtn));

      await waitFor(() => {
        expect(screen.getByText('Configuration saved successfully')).toBeInTheDocument();
      });
    });

    it('status alert can be closed', async () => {
      setupLakebaseMocks();
      mockApiClient.post.mockResolvedValue({ data: { backend_id: 'lb-new' } });

      await act(async () => renderComponent());
      await waitForLoaded();

      const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
      await act(async () => fireEvent.click(saveBtn));

      await waitFor(() => expect(screen.getByText('Configuration saved successfully')).toBeInTheDocument());

      // Close the alert
      const closeBtn = screen.getByRole('button', { name: /close/i });
      await act(async () => fireEvent.click(closeBtn));
    });
  });

  // =======================================================================
  // Lakebase Table Stats Display
  // =======================================================================

  describe('Lakebase Table Stats', () => {
    it('displays unified memory table stats with Ready chip and row count', async () => {
      setupLakebaseMocks({ tablesInitialized: true, withStats: true });
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(screen.getByText('Memory Tables')).toBeInTheDocument();
        expect(screen.getByText('Active')).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(screen.getAllByText('Ready').length).toBeGreaterThanOrEqual(1);
        expect(screen.getByText('crew_memory')).toBeInTheDocument();
      });
    });

    it('shows Missing chip for non-existent tables', async () => {
      mockMBService.getLakebaseTableStats.mockResolvedValue({
        memory: { table_name: 'crew_memory', exists: false, row_count: 0 },
      });
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: { id: 'lb-1', backend_type: 'lakebase', lakebase_config: { instance_name: 'inst', embedding_dimension: 1024, memory_table: 'crew_memory', tables_initialized: true } },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([{ name: 'inst', state: 'ACTIVE' }]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(screen.getByText('Missing')).toBeInTheDocument();
      });
    });

    it('shows text fallback when tables_initialized but no stats loaded', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: { id: 'lb-1', backend_type: 'lakebase', lakebase_config: { instance_name: 'inst', embedding_dimension: 1024, memory_table: 'crew_memory', tables_initialized: true } },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([{ name: 'inst', state: 'ACTIVE' }]));
        return Promise.resolve({ data: {} });
      });
      mockMBService.getLakebaseTableStats.mockRejectedValueOnce(new Error('no stats'));

      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(screen.getByText(/Click .+Refresh Status.+ to see details/)).toBeInTheDocument();
      });
    });
  });

  // =======================================================================
  // Databricks Config Display
  //
  // CrewAI 1.10+ collapses the per-tier index tables into a single
  // "Memory Index" table plus a "Knowledge Base" table.
  // The Databricks Vector Search UI section itself is currently behind a
  // disabled Collapse, but ConfigurationDisplay/children still render when a
  // saved databricks config carries a workspace_url.
  // =======================================================================

  describe('Databricks Config Display', () => {

    it('verifies resources and updates endpoint statuses', async () => {
      setupDatabricksMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/memory-backend/databricks/verify-resources',
          expect.objectContaining({
            params: expect.objectContaining({ workspace_url: 'https://test.databricks.com' }),
          }),
        );
      });
    });


    it('removes missing endpoints/indexes from config after verify', async () => {
      setupDatabricksMocks({ verifyMissing: ['mem-ep', 'ml.agents.mem'] });
      mockApiClient.put.mockResolvedValue({ data: {} });

      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(mockApiClient.put).toHaveBeenCalled();
      });
    });

    it('handles index info not-found response', async () => {
      setupDatabricksMocks({ indexInfoError: true });
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/memory-backend/databricks/index-info',
          expect.anything(),
        );
      });
    });


  });

  // =======================================================================
  // Databricks Handlers via captured props
  //
  // IndexManagementTable callbacks operate on the unified memory store, so the
  // index type is now 'memory' (saved-config key 'unified') or 'document'.
  // =======================================================================


  // =======================================================================
  // handleSetup (auto-create)
  // =======================================================================


  // =======================================================================
  // handleManualSave
  //
  // Manual config now requires a single `memory_index` plus a `document_index`
  // (the legacy short_term/long_term/entity indexes were unified).
  // =======================================================================


  // =======================================================================
  // Embedding dimension fallback (existing)
  // =======================================================================

  describe('embedding dimension fallback', () => {
    it('EMBEDDING_MODELS default model has dimension 1024', () => {
      const defaultModel = EMBEDDING_MODELS.find(m => m.value === 'databricks-gte-large-en');
      expect(defaultModel).toBeDefined();
      expect(defaultModel!.dimension).toBe(1024);
    });

    it('fallback lookup for default model resolves to 1024', () => {
      const dim = EMBEDDING_MODELS.find(m => m.value === 'databricks-gte-large-en')?.dimension || 1024;
      expect(dim).toBe(1024);
    });

    it('fallback dimension for unknown model is 1024', () => {
      const dim = EMBEDDING_MODELS.find(m => m.value === 'unknown')?.dimension || 1024;
      expect(dim).toBe(1024);
    });

    it('all EMBEDDING_MODELS have dimensions >= 1024', () => {
      EMBEDDING_MODELS.forEach(model => {
        expect(model.dimension).toBeGreaterThanOrEqual(1024);
      });
    });
  });

  // =======================================================================
  // loadLakebaseTableStats error path
  // =======================================================================

  describe('loadLakebaseTableStats', () => {
    it('handles error and sets null', async () => {
      mockMBService.getLakebaseTableStats.mockRejectedValueOnce(new Error('stats fail'));
      setupLakebaseMocks({ tablesInitialized: true });

      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(mockMBService.getLakebaseTableStats).toHaveBeenCalled();
      });
    });
  });

  // =======================================================================
  // Dialog onClose callbacks
  // =======================================================================

  describe('Dialog callbacks', () => {


    it('MemoryRecordsBrowser onClose closes the browser', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();

      const browseBtn = screen.getByRole('button', { name: /Browse Memory/i });
      await act(async () => fireEvent.click(browseBtn));
      await waitFor(() => expect(screen.getByTestId('memory-records-browser')).toHaveAttribute('data-open', 'true'));

      await act(async () => (capturedProps.current.MemoryRecordsBrowser.onClose as Function)());
      await waitFor(() => expect(screen.getByTestId('memory-records-browser')).toHaveAttribute('data-open', 'false'));
    });
  });

  // =======================================================================
  // Additional edge cases for coverage
  // =======================================================================

  describe('Edge cases', () => {




    it('Autocomplete debounce clears previous timeout', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });

      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      // Type first query
      await act(async () => fireEvent.change(input, { target: { value: 'first' } }));
      // Type second query before debounce fires (clears the first timeout)
      await act(async () => fireEvent.change(input, { target: { value: 'second' } }));
      await act(async () => vi.advanceTimersByTime(350));

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/database-management/lakebase/instances',
          expect.objectContaining({ params: expect.objectContaining({ search: 'second' }) }),
        );
      });

      vi.useRealTimers();
    });

    it('Autocomplete onOpen triggers load when list is empty', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();
      await act(async () => fireEvent.click(screen.getByLabelText(/Lakebase \(pgvector\)/i)));

      const input = screen.getByLabelText(/Lakebase Instance/i);
      // Open Autocomplete - should trigger load since list is empty
      await act(async () => {
        fireEvent.focus(input);
        fireEvent.mouseDown(input);
      });

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/database-management/lakebase/instances',
          expect.objectContaining({ params: expect.objectContaining({ page: 1 }) }),
        );
      });
    });

    it('verifyActualResources timer with savedConfig that has workspace_url', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      setupDatabricksMocks();

      await act(async () => renderComponent());
      // Advance past the 500ms timer for verifyActualResources
      await act(async () => vi.advanceTimersByTime(600));
      await waitForLoaded();

      vi.useRealTimers();
    });





    it('isOptionEqualToValue is exercised when selecting a pre-selected instance', async () => {
      // Load a Lakebase config with a selected instance, then open the dropdown
      setupLakebaseMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      // Instance is already selected (my-instance). Open the dropdown.
      const input = screen.getByLabelText(/Lakebase Instance/i);
      await act(async () => { fireEvent.focus(input); fireEvent.mouseDown(input); });

      // Wait for dropdown to show options - isOptionEqualToValue is called by Autocomplete
      await waitFor(() => expect(screen.getByText('my-instance')).toBeInTheDocument());
    });

    it('Lakebase config loads instances with search parameter from state', async () => {
      // The loadLakebaseInstances uses instanceSearch state
      setupLakebaseMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      // Instances are loaded with empty search on mount
      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/database-management/lakebase/instances',
          expect.objectContaining({ params: expect.objectContaining({ page: 1, page_size: 30 }) }),
        );
      });
    });








  });

  // =======================================================================
  // Additional coverage: NOT_FOUND endpoint statuses, no-backend_id paths,
  // setup-built minimal configs, and Lakebase initialize failure branch.
  // =======================================================================

  describe('Coverage completion', () => {
    /**
     * Build a Databricks config where verify-resources succeeds on the FIRST
     * call (initial load) but returns success:false on every subsequent call.
     * This lets a post-edit savedConfig keep an endpoint whose name is no longer
     * present in the (now-stale) verifiedResources, so the useEffect that maps
     * endpoint statuses takes the NOT_FOUND else-branches.
     */
    function setupVerifyOnceThenFail() {
      let verifyCalls = 0;
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                catalog: 'ml', schema: 'agents',
                endpoint_name: 'mem-ep', document_endpoint_name: 'doc-ep',
                memory_index: 'ml.agents.mem', document_index: 'ml.agents.doc',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          verifyCalls += 1;
          if (verifyCalls === 1) {
            return Promise.resolve({
              data: {
                success: true,
                resources: {
                  endpoints: {
                    'mem-ep': { name: 'mem-ep', state: 'ONLINE', ready: true },
                    'doc-ep': { name: 'doc-ep', state: 'ONLINE', ready: true },
                  },
                  indexes: {
                    'ml.agents.mem': { name: 'ml.agents.mem' },
                    'ml.agents.doc': { name: 'ml.agents.doc' },
                  },
                },
              },
            });
          }
          // Subsequent verifications report failure so no removal happens and
          // verifiedResources keeps the original (now-stale) endpoint set.
          return Promise.resolve({ data: { success: false } });
        }
        if (url === '/memory-backend/databricks/index-info') {
          return Promise.resolve({ data: { success: true, doc_count: 0, status: 'ONLINE', ready: true, index_type: 'DELTA_SYNC' } });
        }
        if (url.startsWith('/memory-backend/configs/')) {
          return Promise.resolve({ data: { id: 'db-1', databricks_config: { embedding_dimension: 1024 } } });
        }
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
    }






    it('Initialize Tables shows error when the service reports failure', async () => {
      setupLakebaseMocks();
      mockMBService.initializeLakebaseTables.mockResolvedValueOnce({ success: false, message: 'pgvector not enabled' });

      await act(async () => renderComponent());
      await waitForLoaded();

      const initBtn = screen.getByRole('button', { name: /Initialize Tables/i });
      await act(async () => fireEvent.click(initBtn));

      await waitFor(() => {
        expect(screen.getByText('pgvector not enabled')).toBeInTheDocument();
      });
      // The failure branch returns before attempting to save the config.
      expect(mockApiClient.post).not.toHaveBeenCalledWith('/memory-backend/lakebase/save-config', expect.anything());
    });

  });
});
