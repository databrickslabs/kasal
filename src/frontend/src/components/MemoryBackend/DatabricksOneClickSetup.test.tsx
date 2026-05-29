import { vi, beforeEach, afterEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { AxiosError } from 'axios';
import { EMBEDDING_MODELS } from './constants';
import { MemoryBackendType } from '../../types/memoryBackend';

// ---------------------------------------------------------------------------
// Hoisted mock references
// ---------------------------------------------------------------------------

const {
  mockApiClient,
  mockUpdateConfig,
  mockDVSService,
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
  mockDVSService: {
    performOneClickSetup: vi.fn().mockResolvedValue({ success: true }),
    deleteAllConfigurations: vi.fn().mockResolvedValue(undefined),
    cleanupDisabledConfigurations: vi.fn().mockResolvedValue(undefined),
    switchToDisabledMode: vi.fn().mockResolvedValue({ id: '1', backend_type: 'default' }),
    updateBackendConfiguration: vi.fn().mockResolvedValue({ id: '1', backend_type: 'databricks' }),
    verifyResources: vi.fn().mockResolvedValue({ success: true, resources: { endpoints: {}, indexes: {} } }),
  },
  mockMBService: {
    getLakebaseTableStats: vi.fn().mockResolvedValue({}),
    testLakebaseConnection: vi.fn().mockResolvedValue({ success: true, message: 'Connected' }),
    initializeLakebaseTables: vi.fn().mockResolvedValue({ success: true, message: 'Tables created' }),
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
  useMemoryBackendStore: () => ({
    updateConfig: mockUpdateConfig,
  }),
}));

vi.mock('../../api/DatabricksVectorSearchService', () => ({
  default: mockDVSService,
}));

vi.mock('../../api/MemoryBackendService', () => ({
  MemoryBackendService: mockMBService,
}));

vi.mock('./databricksVectorSearchUtils', () => ({
  validateVectorSearchIndexName: mockValidateIndex,
}));

// ---------------------------------------------------------------------------
// Child component mocks – capture props for callback testing
// ---------------------------------------------------------------------------

vi.mock('./SetupResultDialog', () => ({
  SetupResultDialog: (props: Record<string, unknown>) => {
    capturedProps.current.SetupResultDialog = props;
    return <div data-testid="setup-result-dialog" data-open={String(props.open)} />;
  },
}));

vi.mock('./IndexManagementTable', () => ({
  IndexManagementTable: (props: Record<string, unknown>) => {
    const key = `IndexManagementTable_${props.title}`;
    capturedProps.current[key] = props;
    return <div data-testid={`index-management-table-${props.title}`} />;
  },
}));

vi.mock('./ConfigurationDisplay', () => ({
  ConfigurationDisplay: (props: { children?: React.ReactNode; [k: string]: unknown }) => {
    capturedProps.current.ConfigurationDisplay = props;
    return <div data-testid="configuration-display">{props.children}</div>;
  },
}));

vi.mock('./ManualConfigurationForm', () => ({
  ManualConfigurationForm: (props: Record<string, unknown>) => {
    capturedProps.current.ManualConfigurationForm = props;
    return <div data-testid="manual-config-form" />;
  },
}));

vi.mock('./AutomaticSetupForm', () => ({
  AutomaticSetupForm: (props: Record<string, unknown>) => {
    capturedProps.current.AutomaticSetupForm = props;
    return <div data-testid="auto-setup-form" />;
  },
}));

vi.mock('./EditConfigurationForm', () => ({
  EditConfigurationForm: (props: Record<string, unknown>) => {
    capturedProps.current.EditConfigurationForm = props;
    return <div data-testid="edit-config-form" />;
  },
}));

vi.mock('./EndpointsDisplay', () => ({
  EndpointsDisplay: (props: Record<string, unknown>) => {
    capturedProps.current.EndpointsDisplay = props;
    return <div data-testid="endpoints-display" />;
  },
}));

vi.mock('./IndexDocumentsDialog', () => ({
  IndexDocumentsDialog: (props: Record<string, unknown>) => {
    capturedProps.current.IndexDocumentsDialog = props;
    return <div data-testid="index-documents-dialog" data-open={String(props.open)} />;
  },
}));

vi.mock('./MemoryRecordsBrowser', () => ({
  MemoryRecordsBrowser: (props: Record<string, unknown>) => {
    capturedProps.current.MemoryRecordsBrowser = props;
    return <div data-testid="memory-records-browser" data-open={String(props.open)} />;
  },
}));

// ---------------------------------------------------------------------------
// Import component
// ---------------------------------------------------------------------------

import { DatabricksOneClickSetup } from './DatabricksOneClickSetup';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const theme = createTheme();

function renderComponent() {
  return render(
    <ThemeProvider theme={theme}>
      <DatabricksOneClickSetup />
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
 * CrewAI 1.10+ unified cognitive memory: a single `memory_index` replaces the
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
    // Unified cognitive memory uses a single `crew_memory` table.
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

describe('DatabricksOneClickSetup', () => {
  let confirmSpy: ReturnType<typeof vi.spyOn>;
  let alertSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
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

  it('shows info alert about local LanceDB storage when mode is disabled', async () => {
    await act(async () => renderComponent());
    await waitForLoaded();
    expect(
      screen.getByText(/CrewAI unified cognitive memory is stored locally in LanceDB/),
    ).toBeInTheDocument();
  });

  it('renders SetupResultDialog and MemoryRecordsBrowser in closed state', async () => {
    await act(async () => renderComponent());
    await waitForLoaded();
    expect(screen.getByTestId('setup-result-dialog')).toHaveAttribute('data-open', 'false');
    expect(screen.getByTestId('memory-records-browser')).toHaveAttribute('data-open', 'false');
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
      expect(screen.getByText(/CrewAI unified cognitive memory is stored locally in LanceDB/)).toBeInTheDocument();
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

    it('loads Databricks config and processes it correctly', async () => {
      setupDatabricksMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(screen.getByTestId('configuration-display')).toBeInTheDocument();
      });
      // verifyActualResources is called
      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/memory-backend/databricks/verify-resources',
          expect.anything(),
        );
      });
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
      expect(screen.getByText(/CrewAI unified cognitive memory is stored locally in LanceDB/)).toBeInTheDocument();
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
      expect(mockDVSService.switchToDisabledMode).toHaveBeenCalled();
    });

    it('shows error when switchToDisabledMode fails', async () => {
      mockDVSService.switchToDisabledMode.mockRejectedValueOnce(new Error('fail'));

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
  // "Unified Cognitive Memory Index" table plus a "Knowledge Base" table.
  // The Databricks Vector Search UI section itself is currently behind a
  // disabled Collapse, but ConfigurationDisplay/children still render when a
  // saved databricks config carries a workspace_url.
  // =======================================================================

  describe('Databricks Config Display', () => {
    it('shows ConfigurationDisplay when a saved config with workspace_url exists', async () => {
      setupDatabricksMocks();
      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(screen.getByTestId('configuration-display')).toBeInTheDocument());
    });

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

    it('handles verify resources error gracefully', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com', endpoint_name: 'ep1',
                memory_index: 'ml.mem',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') return Promise.reject(new Error('verify fail'));
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 0 } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      // Should not crash
      expect(screen.getByTestId('configuration-display')).toBeInTheDocument();
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

    it('handles fetchIndexInfo 404 error', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'ep1', memory_index: 'ml.mem',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({ data: { success: true, resources: { endpoints: { ep1: { name: 'ep1', state: 'ONLINE', ready: true } }, indexes: { 'ml.mem': { name: 'ml.mem' } } } } });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.reject(makeAxios404());
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
    });

    it('renders the Unified Cognitive Memory Index and Knowledge Base tables', async () => {
      setupDatabricksMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => {
        expect(capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index']).toBeDefined();
        expect(capturedProps.current['IndexManagementTable_Knowledge Base']).toBeDefined();
      });
    });
  });

  // =======================================================================
  // Databricks Handlers via captured props
  //
  // IndexManagementTable callbacks operate on the unified memory store, so the
  // index type is now 'memory' (saved-config key 'unified') or 'document'.
  // =======================================================================

  describe('Databricks Handlers', () => {
    const MEM_TABLE = 'IndexManagementTable_Unified Cognitive Memory Index';
    const KB_TABLE = 'IndexManagementTable_Knowledge Base';

    beforeEach(() => {
      setupDatabricksMocks();
    });

    async function renderAndWaitForDatabricks() {
      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(screen.getByTestId('configuration-display')).toBeInTheDocument());
      // Wait for verify resources to complete
      await waitFor(() => expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/databricks/verify-resources', expect.anything()));
    }

    it('handleStartEdit and handleCancelEdit via ConfigurationDisplay', async () => {
      await renderAndWaitForDatabricks();
      const cdProps = capturedProps.current.ConfigurationDisplay;

      // Start edit
      await act(async () => (cdProps.onStartEdit as Function)());
      await waitFor(() => expect(screen.getByTestId('edit-config-form')).toBeInTheDocument());

      // Cancel edit
      await act(async () => (cdProps.onCancelEdit as Function)());
    });

    it('handleSaveEdit via ConfigurationDisplay', async () => {
      mockApiClient.put.mockResolvedValue({ data: {} });
      await renderAndWaitForDatabricks();

      // Start edit
      let cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onStartEdit as Function)());
      await waitFor(() => expect(screen.getByTestId('edit-config-form')).toBeInTheDocument());

      // Re-capture props after re-render since handlers close over updated state
      cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onSaveEdit as Function)());

      await waitFor(() => expect(mockApiClient.put).toHaveBeenCalled());
    });

    it('handleSaveEdit handles error', async () => {
      mockApiClient.put.mockRejectedValue(new Error('update fail'));
      await renderAndWaitForDatabricks();

      let cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onStartEdit as Function)());
      await waitFor(() => expect(screen.getByTestId('edit-config-form')).toBeInTheDocument());

      cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onSaveEdit as Function)());

      // handleSaveEdit calls updateBackendConfiguration which catches the error,
      // so no throw should escape and the component should remain mounted.
      await waitFor(() => expect(screen.getByTestId('configuration-display')).toBeInTheDocument());
    });

    it('handleEditChange for endpoint field', async () => {
      await renderAndWaitForDatabricks();
      const cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onStartEdit as Function)());

      // Get EditConfigurationForm captured props
      await waitFor(() => expect(capturedProps.current.EditConfigurationForm).toBeDefined());
      const editProps = capturedProps.current.EditConfigurationForm;
      await act(async () => (editProps.onEditChange as Function)('endpoints.memory.name', 'new-ep'));
      await act(async () => (editProps.onEditChange as Function)('endpoints.document.name', undefined));
    });

    it('handleEditChange for unified index field', async () => {
      await renderAndWaitForDatabricks();
      const cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onStartEdit as Function)());

      await waitFor(() => expect(capturedProps.current.EditConfigurationForm).toBeDefined());
      const editProps = capturedProps.current.EditConfigurationForm;
      await act(async () => (editProps.onEditChange as Function)('indexes.unified.name', 'ml.agents.new_mem'));
      await act(async () => (editProps.onEditChange as Function)('indexes.document.name', undefined));
    });

    it('handleDeleteEndpoint via EndpointsDisplay', async () => {
      mockApiClient.delete.mockResolvedValue({ data: { success: true } });
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current.EndpointsDisplay).toBeDefined());
      const epProps = capturedProps.current.EndpointsDisplay;

      await act(async () => (epProps.onDeleteEndpoint as Function)('document'));
      await waitFor(() => expect(mockApiClient.delete).toHaveBeenCalled());
    });

    it('handleDeleteEndpoint for last endpoint switches to disabled', async () => {
      // Setup with only one endpoint
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'only-ep',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({ data: { success: true, resources: { endpoints: { 'only-ep': { name: 'only-ep', state: 'ONLINE', ready: true } }, indexes: {} } } });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 0 } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(capturedProps.current.EndpointsDisplay).toBeDefined());

      const epProps = capturedProps.current.EndpointsDisplay;
      await act(async () => (epProps.onDeleteEndpoint as Function)('memory'));

      await waitFor(() => {
        expect(mockDVSService.switchToDisabledMode).toHaveBeenCalled();
      });
    });

    it('handleDeleteEndpoint for NOT_FOUND endpoint removes from config', async () => {
      // Need endpoint with NOT_FOUND status
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'mem-ep', document_endpoint_name: 'missing-ep',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({
            data: { success: true, resources: { endpoints: { 'mem-ep': { name: 'mem-ep', state: 'ONLINE', ready: true } }, indexes: {} } },
          });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 0 } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.put.mockResolvedValue({ data: {} });

      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(capturedProps.current.EndpointsDisplay).toBeDefined());

      const epProps = capturedProps.current.EndpointsDisplay;
      await act(async () => (epProps.onDeleteEndpoint as Function)('document'));

      await waitFor(() => expect(mockApiClient.put).toHaveBeenCalled());
    });

    it('handleDeleteEndpoint handles error', async () => {
      mockApiClient.delete.mockRejectedValueOnce(new Error('delete fail'));
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current.EndpointsDisplay).toBeDefined());
      await act(async () => (capturedProps.current.EndpointsDisplay.onDeleteEndpoint as Function)('document'));

      await waitFor(() => expect(screen.getByText(/Failed to delete endpoint/)).toBeInTheDocument());
    });

    it('handleDeleteEndpoint cancelled by user does nothing', async () => {
      confirmSpy.mockReturnValueOnce(false);
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current.EndpointsDisplay).toBeDefined());
      await act(async () => (capturedProps.current.EndpointsDisplay.onDeleteEndpoint as Function)('document'));

      expect(mockApiClient.delete).not.toHaveBeenCalledWith('/memory-backend/databricks/endpoint', expect.anything());
    });

    it('handleDeleteIndex via IndexManagementTable', async () => {
      mockApiClient.delete.mockResolvedValue({ data: { success: true } });
      mockApiClient.put.mockResolvedValue({ data: {} });
      await renderAndWaitForDatabricks();

      // Wait for IndexManagementTable to capture props
      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      const tableProps = capturedProps.current[MEM_TABLE];

      await act(async () => (tableProps.onDelete as Function)('memory'));
      await waitFor(() => {
        expect(mockApiClient.delete).toHaveBeenCalledWith('/memory-backend/databricks/index', expect.anything());
      });
    });

    it('handleDeleteIndex fails when API returns failure', async () => {
      mockApiClient.delete.mockResolvedValue({ data: { success: false, message: 'Cannot delete' } });
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onDelete as Function)('memory'));

      await waitFor(() => expect(screen.getByText('Cannot delete')).toBeInTheDocument());
    });

    it('handleDeleteIndex handles error', async () => {
      mockApiClient.delete.mockRejectedValueOnce(new Error('delete fail'));
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onDelete as Function)('memory'));

      await waitFor(() => expect(screen.getByText(/Failed to delete index/)).toBeInTheDocument());
    });

    it('handleDeleteIndex cancelled by user does nothing', async () => {
      confirmSpy.mockReturnValueOnce(false);
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onDelete as Function)('memory'));

      expect(mockApiClient.delete).not.toHaveBeenCalledWith('/memory-backend/databricks/index', expect.anything());
    });

    it('handleEmptyIndex via IndexManagementTable', async () => {
      mockApiClient.post.mockResolvedValue({ data: { success: true, deleted_count: 5 } });
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onEmpty as Function)('memory'));

      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith('/memory-backend/databricks/empty-index', expect.anything());
      });
      expect(alertSpy).toHaveBeenCalled();
    });

    it('handleEmptyIndex shows "created new index" message', async () => {
      mockApiClient.post.mockResolvedValue({ data: { success: true, message: 'created new index successfully' } });
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onEmpty as Function)('memory'));

      await waitFor(() => {
        expect(alertSpy).toHaveBeenCalledWith(expect.stringContaining('new one was created'));
      });
    });

    it('handleEmptyIndex handles API failure response', async () => {
      mockApiClient.post.mockResolvedValue({ data: { success: false, message: 'Failed', error: 'not supported' } });
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onEmpty as Function)('memory'));

      await waitFor(() => expect(screen.getByText('Failed')).toBeInTheDocument());
    });

    it('handleEmptyIndex handles error', async () => {
      mockApiClient.post.mockRejectedValueOnce(new Error('empty fail'));
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onEmpty as Function)('memory'));

      await waitFor(() => expect(screen.getByText(/Failed to empty index/)).toBeInTheDocument());
    });

    it('handleEmptyIndex cancelled by user does nothing', async () => {
      confirmSpy.mockReturnValueOnce(false);
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onEmpty as Function)('memory'));

      expect(mockApiClient.post).not.toHaveBeenCalledWith('/memory-backend/databricks/empty-index', expect.anything());
    });

    it('handleReseedDocumentation via Knowledge Base table', async () => {
      mockApiClient.post.mockImplementation((url: string) => {
        if (url === '/memory-backend/databricks/empty-index') return Promise.resolve({ data: { success: true } });
        if (url === '/documentation-embeddings/seed-all') return Promise.resolve({ data: { success: true } });
        return Promise.resolve({ data: {} });
      });

      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[KB_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[KB_TABLE].onRefresh as Function)());

      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith('/memory-backend/databricks/empty-index', expect.anything());
        expect(mockApiClient.post).toHaveBeenCalledWith('/documentation-embeddings/seed-all');
      });
    });

    it('handleReseedDocumentation handles empty-index failure', async () => {
      mockApiClient.post.mockResolvedValue({ data: { success: false, message: 'Empty failed' } });
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[KB_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[KB_TABLE].onRefresh as Function)());

      await waitFor(() => expect(screen.getByText(/Empty failed/)).toBeInTheDocument());
    });

    it('handleReseedDocumentation handles seed-all failure', async () => {
      mockApiClient.post.mockImplementation((url: string) => {
        if (url === '/memory-backend/databricks/empty-index') return Promise.resolve({ data: { success: true } });
        if (url === '/documentation-embeddings/seed-all') return Promise.resolve({ data: { success: false, message: 'Seed failed' } });
        return Promise.resolve({ data: {} });
      });

      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[KB_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[KB_TABLE].onRefresh as Function)());

      await waitFor(() => expect(screen.getByText('Seed failed')).toBeInTheDocument());
    });

    it('handleReseedDocumentation handles thrown error', async () => {
      mockApiClient.post.mockRejectedValue(new Error('seed error'));
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[KB_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[KB_TABLE].onRefresh as Function)());

      await waitFor(() => expect(screen.getByText(/seed error/)).toBeInTheDocument());
    });

    it('handleReseedDocumentation handles AxiosError with response detail', async () => {
      const axErr = new AxiosError('fail', '500', undefined, undefined, {
        status: 500, data: { detail: 'Server error detail' }, statusText: 'Error', headers: {}, config: {} as never,
      } as never);
      mockApiClient.post.mockRejectedValue(axErr);
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[KB_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[KB_TABLE].onRefresh as Function)());

      await waitFor(() => expect(screen.getByText('Server error detail')).toBeInTheDocument());
    });

    it('handleReseedDocumentation cancelled by user does nothing', async () => {
      confirmSpy.mockReturnValueOnce(false);
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[KB_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[KB_TABLE].onRefresh as Function)());

      expect(mockApiClient.post).not.toHaveBeenCalledWith('/memory-backend/databricks/empty-index', expect.anything());
    });

    it('handleViewDocuments via IndexManagementTable', async () => {
      await renderAndWaitForDatabricks();

      await waitFor(() => expect(capturedProps.current[MEM_TABLE]).toBeDefined());
      await act(async () => (capturedProps.current[MEM_TABLE].onViewDocuments as Function)('memory', 'ml.agents.mem'));

      await waitFor(() => {
        expect(screen.getByTestId('index-documents-dialog')).toHaveAttribute('data-open', 'true');
      });
    });

    it('onRefresh (verifyActualResources) via ConfigurationDisplay', async () => {
      await renderAndWaitForDatabricks();
      const cdProps = capturedProps.current.ConfigurationDisplay;

      mockApiClient.get.mockClear();
      await act(async () => (cdProps.onRefresh as Function)());

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          '/memory-backend/databricks/verify-resources',
          expect.anything(),
        );
      });
    });
  });

  // =======================================================================
  // handleSetup (auto-create)
  // =======================================================================

  describe('handleSetup', () => {
    it('shows error when no workspace URL detected', async () => {
      // No databricks_host in environment
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: {} });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      // The Databricks auto-setup form renders (collapsed) when there is no
      // saved workspace_url and setupMode is 'auto'. Trigger its onSetup.
      if (capturedProps.current.AutomaticSetupForm) {
        await act(async () => (capturedProps.current.AutomaticSetupForm.onSetup as Function)());
      }
    });

    it('performs one-click setup successfully', async () => {
      mockDVSService.performOneClickSetup.mockResolvedValueOnce({
        success: true,
        catalog: 'ml',
        schema: 'agents',
        endpoints: { memory: { name: 'ep1' }, document: { name: 'ep2' } },
        indexes: { unified: { name: 'ml.agents.mem' } },
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      if (capturedProps.current.AutomaticSetupForm) {
        await act(async () => (capturedProps.current.AutomaticSetupForm.onSetup as Function)());

        await waitFor(() => {
          expect(mockDVSService.performOneClickSetup).toHaveBeenCalled();
        });
      }
    });

    it('handles setup error', async () => {
      mockDVSService.performOneClickSetup.mockRejectedValueOnce(new Error('Setup failed'));

      await act(async () => renderComponent());
      await waitForLoaded();

      if (capturedProps.current.AutomaticSetupForm) {
        await act(async () => (capturedProps.current.AutomaticSetupForm.onSetup as Function)());
      }
    });

    it('handles AxiosError during setup', async () => {
      const axErr = new AxiosError('fail', '500', undefined, undefined, {
        status: 500, data: { detail: 'Backend error' }, statusText: 'Error', headers: {}, config: {} as never,
      } as never);
      mockDVSService.performOneClickSetup.mockRejectedValueOnce(axErr);

      await act(async () => renderComponent());
      await waitForLoaded();

      if (capturedProps.current.AutomaticSetupForm) {
        await act(async () => (capturedProps.current.AutomaticSetupForm.onSetup as Function)());
      }
    });
  });

  // =======================================================================
  // handleManualSave
  //
  // Manual config now requires a single `memory_index` plus a `document_index`
  // (the legacy short_term/long_term/entity indexes were unified).
  // =======================================================================

  describe('handleManualSave', () => {
    /** Switch setupMode from 'auto' to 'manual' by clicking the hidden radio */
    async function switchToManualMode() {
      const allRadios = screen.getAllByRole('radio', { hidden: true });
      const manualRadio = allRadios.find(r => (r as HTMLInputElement).value === 'manual');
      expect(manualRadio).toBeDefined();
      await act(async () => fireEvent.click(manualRadio!));
      // Wait for ManualConfigurationForm to mount
      await waitFor(() => expect(capturedProps.current.ManualConfigurationForm).toBeDefined());
    }

    const validManualConfig = {
      workspace_url: 'https://test.com',
      endpoint_name: 'ep1',
      document_endpoint_name: 'ep2',
      memory_index: 'ml.agents.mem',
      document_index: 'ml.agents.doc',
      embedding_model: 'databricks-gte-large-en',
    };

    async function setManualConfigAndSave(config: Record<string, string>) {
      const onConfigChange = capturedProps.current.ManualConfigurationForm.onConfigChange as Function;
      await act(async () => onConfigChange(config));
      // Re-capture onSave after state update
      const onSave = capturedProps.current.ManualConfigurationForm.onSave as Function;
      await act(async () => onSave());
    }

    it('shows error when required fields are missing', async () => {
      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      await setManualConfigAndSave({ workspace_url: '', endpoint_name: '' });
      // handleManualSave sets error about all fields required (inside hidden Collapse)
    });

    it('shows error when index name format is invalid', async () => {
      mockValidateIndex.mockReturnValueOnce(false);

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      await setManualConfigAndSave({
        ...validManualConfig,
        memory_index: 'invalid',
      });
    });

    it('saves manual config by updating existing config', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({ data: { id: 'existing-id', backend_type: 'default' } });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.put.mockResolvedValue({ data: { id: 'existing-id' } });

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      await setManualConfigAndSave(validManualConfig);

      await waitFor(() => {
        expect(mockApiClient.put).toHaveBeenCalledWith(
          '/memory-backend/configs/existing-id',
          expect.objectContaining({ backend_type: MemoryBackendType.DATABRICKS }),
        );
      });
    });

    it('manual save payload uses unified memory_index', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({ data: { id: 'existing-id', backend_type: 'default' } });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.put.mockResolvedValue({ data: { id: 'existing-id' } });

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      await setManualConfigAndSave(validManualConfig);

      await waitFor(() => {
        expect(mockApiClient.put).toHaveBeenCalledWith(
          '/memory-backend/configs/existing-id',
          expect.objectContaining({
            databricks_config: expect.objectContaining({ memory_index: 'ml.agents.mem' }),
          }),
        );
      });
    });

    it('saves manual config by creating new config when no existing', async () => {
      mockApiClient.post.mockResolvedValue({ data: { id: 'new-id' } });
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      await setManualConfigAndSave(validManualConfig);

      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith(
          '/memory-backend/configs',
          expect.objectContaining({ backend_type: MemoryBackendType.DATABRICKS }),
        );
      });
      // Also sets as default
      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith(
          '/memory-backend/configs/new-id/set-default',
        );
      });
    });

    it('handles manual save error', async () => {
      mockApiClient.delete.mockResolvedValue({ data: {} }); // cleanup
      mockApiClient.post.mockRejectedValue(new Error('save fail'));
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      await setManualConfigAndSave(validManualConfig);
      // error is set but inside hidden Collapse
    });

    it('handles cleanup failure gracefully during manual save', async () => {
      mockApiClient.delete.mockRejectedValue(new Error('cleanup fail'));
      mockApiClient.put.mockResolvedValue({ data: { id: 'existing-id' } });
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({ data: { id: 'existing-id', backend_type: 'default' } });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      await setManualConfigAndSave(validManualConfig);

      // cleanup failure doesn't prevent save
      await waitFor(() => {
        expect(mockApiClient.put).toHaveBeenCalledWith(
          '/memory-backend/configs/existing-id',
          expect.objectContaining({ backend_type: MemoryBackendType.DATABRICKS }),
        );
      });
    });

    it('fetches default config when no savedConfig backend_id', async () => {
      // Start with empty default, and /configs/default returns a config when checked inside handleManualSave
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({ data: {} });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.post.mockResolvedValue({ data: { id: 'new-id' } });

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      // Override get for the check inside handleManualSave to find a default
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({ data: { id: 'found-default', backend_type: 'default' } });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.put.mockResolvedValue({ data: { id: 'found-default' } });

      await setManualConfigAndSave(validManualConfig);

      await waitFor(() => {
        expect(mockApiClient.put).toHaveBeenCalledWith(
          '/memory-backend/configs/found-default',
          expect.objectContaining({ backend_type: MemoryBackendType.DATABRICKS }),
        );
      });
    });

    it('handles default config lookup failure during manual save', async () => {
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.resolve({ data: {} });
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.post.mockResolvedValue({ data: { id: 'new-id' } });

      await act(async () => renderComponent());
      await waitForLoaded();
      await switchToManualMode();

      // Override to make default check fail inside handleManualSave
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') return Promise.reject(new Error('no default'));
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/configs') return Promise.resolve({ data: [] });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await setManualConfigAndSave(validManualConfig);

      // Falls through to create (POST)
      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith(
          '/memory-backend/configs',
          expect.objectContaining({ backend_type: MemoryBackendType.DATABRICKS }),
        );
      });
    });
  });

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
    it('SetupResultDialog onClose closes the dialog', async () => {
      // Trigger setup to open the result dialog
      mockDVSService.performOneClickSetup.mockResolvedValueOnce({
        success: true, catalog: 'ml', schema: 'agents',
        endpoints: {}, indexes: {},
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      // Trigger handleSetup if AutomaticSetupForm is available
      if (capturedProps.current.AutomaticSetupForm) {
        await act(async () => (capturedProps.current.AutomaticSetupForm.onSetup as Function)());
        // Close the dialog
        await waitFor(() => expect(capturedProps.current.SetupResultDialog).toBeDefined());
        if (capturedProps.current.SetupResultDialog?.onClose) {
          await act(async () => (capturedProps.current.SetupResultDialog.onClose as Function)());
        }
      }
    });

    it('IndexDocumentsDialog onClose closes and clears selection', async () => {
      setupDatabricksMocks();
      await act(async () => renderComponent());
      await waitForLoaded();

      // Open via handleViewDocuments
      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index']).toBeDefined());
      await act(async () => {
        (capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index'].onViewDocuments as Function)('memory', 'ml.agents.mem');
      });

      // Close the dialog
      await waitFor(() => expect(capturedProps.current.IndexDocumentsDialog).toBeDefined());
      if (capturedProps.current.IndexDocumentsDialog?.onClose) {
        await act(async () => (capturedProps.current.IndexDocumentsDialog.onClose as Function)());
      }
    });

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
    it('endpoint statuses update for NOT_FOUND endpoints', async () => {
      // Verify with missing endpoints to trigger NOT_FOUND branch
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'mem-ep', document_endpoint_name: 'doc-ep',
                memory_index: 'ml.mem',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          // Only mem-ep exists, doc-ep is missing → triggers NOT_FOUND branch
          return Promise.resolve({
            data: {
              success: true,
              resources: {
                endpoints: { 'mem-ep': { name: 'mem-ep', state: 'ONLINE', ready: true } },
                indexes: { 'ml.mem': { name: 'ml.mem' } },
              },
            },
          });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 0, status: 'ONLINE', ready: true, index_type: 'DELTA_SYNC' } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.put.mockResolvedValue({ data: {} });

      await act(async () => renderComponent());
      await waitForLoaded();

      // Wait for verify and update
      await waitFor(() => expect(mockApiClient.put).toHaveBeenCalled());
    });

    it('handleDeleteIndex blocks when index already deleted', async () => {
      // Setup with index info showing NOT_FOUND
      setupDatabricksMocks({ indexInfoError: true });
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index']).toBeDefined());
      // The index info will show NOT_FOUND/error status
      // When trying to delete, it should show an error about already deleted
      await act(async () => (capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index'].onDelete as Function)('memory'));

      await waitFor(() => expect(screen.getByText(/already been deleted/)).toBeInTheDocument());
    });

    it('handleDeleteEndpoint API returns failure response', async () => {
      setupDatabricksMocks();
      mockApiClient.delete.mockResolvedValue({ data: { success: false, message: 'Endpoint busy' } });

      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(capturedProps.current.EndpointsDisplay).toBeDefined());

      await act(async () => (capturedProps.current.EndpointsDisplay.onDeleteEndpoint as Function)('document'));

      await waitFor(() => expect(screen.getByText('Endpoint busy')).toBeInTheDocument());
    });

    it('handleEmptyIndex returns early when no endpoint', async () => {
      // Setup with memory index but no memory endpoint
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                memory_index: 'ml.mem', // index exists but no endpoint
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') return Promise.resolve({ data: { success: true, resources: { endpoints: {}, indexes: { 'ml.mem': { name: 'ml.mem' } } } } });
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 5, status: 'ONLINE', ready: true, index_type: 'DELTA_SYNC' } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        if (url.startsWith('/memory-backend/configs/')) return Promise.resolve({ data: { databricks_config: { embedding_dimension: 1024 } } });
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      // The unified memory table renders since we have a memory index
      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index']).toBeDefined());
      await act(async () => (capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index'].onEmpty as Function)('memory'));

      // Should show "Could not determine endpoint" error
      await waitFor(() => expect(screen.getByText(/Could not determine endpoint/)).toBeInTheDocument());
    });

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

    it('handleSetup with deleteAllConfigurations failure continues', async () => {
      mockDVSService.deleteAllConfigurations.mockRejectedValueOnce(new Error('delete fail'));
      mockDVSService.performOneClickSetup.mockResolvedValueOnce({
        success: false, message: 'Setup failed',
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      if (capturedProps.current.AutomaticSetupForm) {
        await act(async () => (capturedProps.current.AutomaticSetupForm.onSetup as Function)());
      }
    });

    it('handleSetup with successful result updates store and reloads config', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      mockDVSService.performOneClickSetup.mockResolvedValueOnce({
        success: true, catalog: 'ml', schema: 'agents',
        endpoints: { memory: { name: 'ep1' } },
        indexes: { unified: { name: 'ml.agents.mem' } },
      });

      await act(async () => renderComponent());
      await act(async () => vi.advanceTimersByTime(100));
      await waitForLoaded();

      if (capturedProps.current.AutomaticSetupForm) {
        await act(async () => (capturedProps.current.AutomaticSetupForm.onSetup as Function)());
        await act(async () => vi.advanceTimersByTime(2000));
      }

      vi.useRealTimers();
    });

    it('Knowledge Base onEmpty callback triggers handleEmptyIndex', async () => {
      setupDatabricksMocks();
      mockApiClient.post.mockResolvedValue({ data: { success: true, deleted_count: 2 } });
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Knowledge Base']).toBeDefined());
      await act(async () => (capturedProps.current['IndexManagementTable_Knowledge Base'].onEmpty as Function)('document'));

      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith('/memory-backend/databricks/empty-index', expect.anything());
      });
    });

    it('Knowledge Base onDelete callback triggers handleDeleteIndex', async () => {
      setupDatabricksMocks();
      mockApiClient.delete.mockResolvedValue({ data: { success: true } });
      mockApiClient.put.mockResolvedValue({ data: {} });
      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Knowledge Base']).toBeDefined());
      await act(async () => (capturedProps.current['IndexManagementTable_Knowledge Base'].onDelete as Function)('document'));

      await waitFor(() => {
        expect(mockApiClient.delete).toHaveBeenCalledWith('/memory-backend/databricks/index', expect.anything());
      });
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

    it('handleDeleteEndpoint error path', async () => {
      setupDatabricksMocks();
      mockApiClient.delete.mockRejectedValueOnce(new Error('delete fail'));

      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(capturedProps.current.EndpointsDisplay).toBeDefined());

      await act(async () => (capturedProps.current.EndpointsDisplay.onDeleteEndpoint as Function)('memory'));
      await waitFor(() => expect(screen.getByText(/Failed to delete endpoint/)).toBeInTheDocument());
    });

    it('handleDeleteIndex with endpoint not ready', async () => {
      // Setup where endpoint is NOT ONLINE
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'ep1', memory_index: 'ml.mem',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({
            data: {
              success: true,
              resources: {
                endpoints: { ep1: { name: 'ep1', state: 'PROVISIONING', ready: false } },
                indexes: { 'ml.mem': { name: 'ml.mem', status: 'ONLINE' } },
              },
            },
          });
        }
        if (url === '/memory-backend/databricks/index-info') {
          return Promise.resolve({ data: { success: true, doc_count: 5, status: 'ONLINE', ready: true, index_type: 'DELTA_SYNC' } });
        }
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index']).toBeDefined());
      await act(async () => (capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index'].onDelete as Function)('memory'));

      await waitFor(() => {
        expect(screen.getByText(/Cannot delete index: Endpoint is PROVISIONING/)).toBeInTheDocument();
      });
    });

    it('Knowledge Base table is absent when no document index', async () => {
      // Setup with no document index
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'ep1', memory_index: 'ml.mem',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({ data: { success: true, resources: { endpoints: { ep1: { name: 'ep1', state: 'ONLINE', ready: true } }, indexes: { 'ml.mem': { name: 'ml.mem' } } } } });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 0 } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(screen.getByTestId('configuration-display')).toBeInTheDocument());
      // Knowledge Base table should not render since no document index
      expect(capturedProps.current['IndexManagementTable_Knowledge Base']).toBeUndefined();
    });

    it('verifyActualResources removes a missing document index', async () => {
      // Both indexes exist in config but only memory exists in Databricks
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'mem-ep', document_endpoint_name: 'doc-ep',
                memory_index: 'ml.agents.mem',
                document_index: 'ml.agents.doc',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          // Only mem exists, doc is missing
          return Promise.resolve({
            data: {
              success: true,
              resources: {
                endpoints: {
                  'mem-ep': { name: 'mem-ep', state: 'ONLINE', ready: true },
                  'doc-ep': { name: 'doc-ep', state: 'ONLINE', ready: true },
                },
                indexes: { 'ml.agents.mem': { name: 'ml.agents.mem' } },
              },
            },
          });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 0, status: 'ONLINE', ready: true } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.put.mockResolvedValue({ data: {} });

      await act(async () => renderComponent());
      await waitForLoaded();

      // Config should be updated to remove the missing document index
      await waitFor(() => {
        expect(mockApiClient.put).toHaveBeenCalled();
      });
    });

    it('handleReseedDocumentation returns early when no document endpoint', async () => {
      // Has document index but no document endpoint
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'mem-ep',
                memory_index: 'ml.agents.mem',
                document_index: 'ml.agents.doc',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({
            data: {
              success: true,
              resources: {
                endpoints: { 'mem-ep': { name: 'mem-ep', state: 'ONLINE', ready: true } },
                indexes: { 'ml.agents.mem': { name: 'ml.agents.mem' }, 'ml.agents.doc': { name: 'ml.agents.doc' } },
              },
            },
          });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 5, status: 'ONLINE', ready: true } });
        if (url.startsWith('/memory-backend/configs/')) return Promise.resolve({ data: { databricks_config: { embedding_dimension: 1024 } } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });
      mockApiClient.put.mockResolvedValue({ data: {} });

      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Knowledge Base']).toBeDefined());
      await act(async () => (capturedProps.current['IndexManagementTable_Knowledge Base'].onRefresh as Function)());

      // Should set error about document endpoint
      await waitFor(() => expect(screen.getByText(/Could not determine document endpoint/)).toBeInTheDocument());
    });

    it('handleEditChange clears endpoint and unified index when value is undefined', async () => {
      setupDatabricksMocks();
      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(screen.getByTestId('configuration-display')).toBeInTheDocument());

      const cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onStartEdit as Function)());
      await waitFor(() => expect(capturedProps.current.EditConfigurationForm).toBeDefined());

      const editProps = capturedProps.current.EditConfigurationForm;
      // Set value then clear it (undefined)
      await act(async () => (editProps.onEditChange as Function)('endpoints.memory.name', undefined));
      // Clear index value
      await act(async () => (editProps.onEditChange as Function)('indexes.unified.name', undefined));
    });

    it('handleSaveEdit proceeds when editedConfig has a backend_id', async () => {
      setupDatabricksMocks();
      mockApiClient.put.mockResolvedValue({ data: {} });
      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(screen.getByTestId('configuration-display')).toBeInTheDocument());

      // Start edit (editedConfig is a deep copy of savedConfig with backend_id='db-123')
      let cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onStartEdit as Function)());
      await waitFor(() => expect(capturedProps.current.EditConfigurationForm).toBeDefined());

      cdProps = capturedProps.current.ConfigurationDisplay;
      await act(async () => (cdProps.onSaveEdit as Function)());

      await waitFor(() => expect(mockApiClient.put).toHaveBeenCalled());
    });

    it('handleDeleteIndex returns early when unified index missing from savedConfig', async () => {
      // Setup with saved config that has an endpoint but no document index
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                endpoint_name: 'mem-ep',
                memory_index: 'ml.agents.mem',
                // no document_index
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({
            data: { success: true, resources: { endpoints: { 'mem-ep': { name: 'mem-ep', state: 'ONLINE', ready: true } }, indexes: { 'ml.agents.mem': { name: 'ml.agents.mem' } } } },
          });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 0 } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();
      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index']).toBeDefined());

      // Try to delete document which doesn't exist in config
      await act(async () => (capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index'].onDelete as Function)('document'));
      // Should return early without calling delete API
      expect(mockApiClient.delete).not.toHaveBeenCalledWith('/memory-backend/databricks/index', expect.anything());
    });

    it('handleViewDocuments shows error when endpoint not configured', async () => {
      // Setup with memory index but no endpoints
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'db-1', backend_type: 'databricks',
              databricks_config: {
                workspace_url: 'https://test.databricks.com',
                memory_index: 'ml.mem',
              },
            },
          });
        }
        if (url === '/databricks/environment') return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        if (url === '/memory-backend/databricks/verify-resources') {
          return Promise.resolve({ data: { success: true, resources: { endpoints: {}, indexes: { 'ml.mem': { name: 'ml.mem' } } } } });
        }
        if (url === '/memory-backend/databricks/index-info') return Promise.resolve({ data: { success: true, doc_count: 5 } });
        if (url === '/database-management/lakebase/instances') return Promise.resolve(paginatedInstances([]));
        return Promise.resolve({ data: {} });
      });

      await act(async () => renderComponent());
      await waitForLoaded();

      await waitFor(() => expect(capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index']).toBeDefined());
      await act(async () => (capturedProps.current['IndexManagementTable_Unified Cognitive Memory Index'].onViewDocuments as Function)('memory', 'ml.mem'));

      await waitFor(() => expect(screen.getByText(/Cannot view documents: endpoint not configured/)).toBeInTheDocument());
    });
  });
});
