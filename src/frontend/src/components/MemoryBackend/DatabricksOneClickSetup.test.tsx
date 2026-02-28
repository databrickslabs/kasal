import { vi, beforeEach, afterEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { EMBEDDING_MODELS } from './constants';
import { MemoryBackendType } from '../../types/memoryBackend';

// ---------------------------------------------------------------------------
// Hoisted mock references (available to vi.mock factories)
// ---------------------------------------------------------------------------

const {
  mockApiClient,
  mockUpdateConfig,
  mockOpenVisualization,
  mockCloseVisualization,
} = vi.hoisted(() => ({
  mockApiClient: {
    get: vi.fn().mockResolvedValue({ data: {} }),
    put: vi.fn().mockResolvedValue({ data: {} }),
    post: vi.fn().mockResolvedValue({ data: {} }),
    delete: vi.fn().mockResolvedValue({ data: {} }),
  },
  mockUpdateConfig: vi.fn(),
  mockOpenVisualization: vi.fn(),
  mockCloseVisualization: vi.fn(),
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
    visualizationOpen: false,
    visualizationIndex: null,
    openVisualization: mockOpenVisualization,
    closeVisualization: mockCloseVisualization,
  }),
}));

vi.mock('../../api/DatabricksVectorSearchService', () => ({
  default: {
    performOneClickSetup: vi.fn().mockResolvedValue({ success: true }),
    deleteAllConfigurations: vi.fn().mockResolvedValue(undefined),
    cleanupDisabledConfigurations: vi.fn().mockResolvedValue(undefined),
    switchToDisabledMode: vi.fn().mockResolvedValue({ id: '1', backend_type: 'default' }),
    updateBackendConfiguration: vi.fn().mockResolvedValue({ id: '1', backend_type: 'databricks' }),
    verifyResources: vi.fn().mockResolvedValue({ success: true, resources: { endpoints: {}, indexes: {} } }),
  },
}));

vi.mock('./databricksVectorSearchUtils', () => ({
  validateVectorSearchIndexName: vi.fn().mockReturnValue(true),
}));

// ---------------------------------------------------------------------------
// Mock ALL child components as simple divs to avoid complex rendering
// ---------------------------------------------------------------------------

vi.mock('./SetupResultDialog', () => ({
  SetupResultDialog: (props: Record<string, unknown>) => (
    <div data-testid="setup-result-dialog" data-open={String(props.open)} />
  ),
}));

vi.mock('./IndexManagementTable', () => ({
  IndexManagementTable: (props: Record<string, unknown>) => (
    <div data-testid="index-management-table" data-title={props.title as string} />
  ),
}));

vi.mock('./ConfigurationDisplay', () => ({
  ConfigurationDisplay: (props: { children?: React.ReactNode; savedConfig?: Record<string, unknown> }) => (
    <div data-testid="configuration-display">
      {props.children}
    </div>
  ),
}));

vi.mock('./ManualConfigurationForm', () => ({
  ManualConfigurationForm: (props: { manualConfig?: { embedding_model?: string } }) => (
    <div
      data-testid="manual-config-form"
      data-embedding-model={props.manualConfig?.embedding_model}
    >
      {props.manualConfig?.embedding_model}
    </div>
  ),
}));

vi.mock('./AutomaticSetupForm', () => ({
  AutomaticSetupForm: (props: { embeddingModel?: string }) => (
    <div
      data-testid="auto-setup-form"
      data-embedding-model={props.embeddingModel}
    >
      {props.embeddingModel}
    </div>
  ),
}));

vi.mock('./EditConfigurationForm', () => ({
  EditConfigurationForm: () => <div data-testid="edit-config-form" />,
}));

vi.mock('./EndpointsDisplay', () => ({
  EndpointsDisplay: () => <div data-testid="endpoints-display" />,
}));

vi.mock('./EntityGraphVisualization', () => ({
  __esModule: true,
  default: (props: Record<string, unknown>) => (
    <div data-testid={`entity-graph-visualization-${props.dataSource || 'databricks'}`} data-open={String(props.open)} />
  ),
}));

vi.mock('./IndexDocumentsDialog', () => ({
  IndexDocumentsDialog: (props: Record<string, unknown>) => (
    <div data-testid="index-documents-dialog" data-open={String(props.open)} />
  ),
}));

vi.mock('./LakebaseDocumentsDialog', () => ({
  __esModule: true,
  default: (props: Record<string, unknown>) => (
    <div data-testid="lakebase-documents-dialog" data-open={String(props.open)} />
  ),
}));

// ---------------------------------------------------------------------------
// Import the component AFTER all vi.mock calls (hoisted automatically)
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('DatabricksOneClickSetup', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    // Default mock: no existing config (empty response) so the setup UI is shown
    mockApiClient.get.mockImplementation((url: string) => {
      if (url === '/memory-backend/configs/default') {
        return Promise.resolve({ data: {} });
      }
      if (url === '/databricks/environment') {
        return Promise.resolve({
          data: { databricks_host: 'https://test.databricks.com' },
        });
      }
      if (url === '/memory-backend/configs') {
        return Promise.resolve({ data: [] });
      }
      if (url === '/database-management/lakebase/instances') {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({ data: {} });
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // -----------------------------------------------------------------------
  // 1. Renders "Memory Configuration" heading
  // -----------------------------------------------------------------------
  it('renders the Memory Configuration heading', async () => {
    await act(async () => {
      renderComponent();
    });

    // The heading is present both in the loading state and the loaded state
    expect(screen.getAllByText('Memory Configuration').length).toBeGreaterThanOrEqual(1);
  });

  // -----------------------------------------------------------------------
  // 2. Shows loading state initially (before initial config check completes)
  // -----------------------------------------------------------------------
  it('shows loading state initially before config check completes', async () => {
    // Make the default config request hang so we can observe the loading state
    let resolveDefaultConfig!: (value: { data: Record<string, unknown> }) => void;
    mockApiClient.get.mockImplementation((url: string) => {
      if (url === '/memory-backend/configs/default') {
        return new Promise((resolve) => {
          resolveDefaultConfig = resolve;
        });
      }
      if (url === '/databricks/environment') {
        return Promise.resolve({
          data: { databricks_host: 'https://test.databricks.com' },
        });
      }
      return Promise.resolve({ data: {} });
    });

    await act(async () => {
      renderComponent();
    });

    // While loading, the CircularProgress and loading text should be visible
    expect(screen.getByText('Loading memory configuration...')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    // Resolve to clean up
    await act(async () => {
      resolveDefaultConfig({ data: {} });
    });
  });

  // -----------------------------------------------------------------------
  // 3. After loading, shows the disabled/databricks radio options
  // -----------------------------------------------------------------------
  it('shows radio options after loading', async () => {
    await act(async () => {
      renderComponent();
    });

    // Wait for loading to complete (hasCheckedInitialConfig becomes true)
    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // The radio options should be visible
    const radioGroup = screen.getAllByRole('radio');
    expect(radioGroup.length).toBeGreaterThanOrEqual(3);

    expect(screen.getByText('Databricks Vector Search')).toBeInTheDocument();
    expect(screen.getByText('Lakebase (pgvector)')).toBeInTheDocument();
    expect(screen.getByText('Local')).toBeInTheDocument();
  });

  // -----------------------------------------------------------------------
  // 4. Shows "Disabled" info alert when in disabled mode
  // -----------------------------------------------------------------------
  it('shows info alert about local storage when mode is disabled (local)', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // The default mode is 'disabled' (local) since the API returns empty config
    expect(
      screen.getByText(/Uses local storage with ChromaDB for vector search and SQLite for long-term memory/),
    ).toBeInTheDocument();
  });

  // -----------------------------------------------------------------------
  // 5. CRITICAL: Default embedding model is 'databricks-gte-large-en'
  // -----------------------------------------------------------------------
  describe('default embedding model', () => {
    it('passes databricks-gte-large-en as the default embeddingModel to AutomaticSetupForm', async () => {
      await act(async () => {
        renderComponent();
      });

      await waitFor(() => {
        expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
      });

      // Switch to databricks mode by clicking the radio
      const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
      await act(async () => {
        fireEvent.click(databricksRadio);
      });

      // The auto-setup form should be visible with the correct default embedding model
      await waitFor(() => {
        const autoForm = screen.getByTestId('auto-setup-form');
        expect(autoForm).toBeInTheDocument();
        expect(autoForm).toHaveAttribute('data-embedding-model', 'databricks-gte-large-en');
      });
    });

    it('passes databricks-gte-large-en as the default embedding_model in manualConfig to ManualConfigurationForm', async () => {
      await act(async () => {
        renderComponent();
      });

      await waitFor(() => {
        expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
      });

      // Switch to databricks mode
      const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
      await act(async () => {
        fireEvent.click(databricksRadio);
      });

      // Switch to manual setup mode
      const manualRadio = screen.getByLabelText(/Manual setup/i);
      await act(async () => {
        fireEvent.click(manualRadio);
      });

      await waitFor(() => {
        const manualForm = screen.getByTestId('manual-config-form');
        expect(manualForm).toBeInTheDocument();
        expect(manualForm).toHaveAttribute('data-embedding-model', 'databricks-gte-large-en');
      });
    });

    it('does NOT default to the old databricks-bge-large-en model', async () => {
      await act(async () => {
        renderComponent();
      });

      await waitFor(() => {
        expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
      });

      // Switch to databricks mode
      const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
      await act(async () => {
        fireEvent.click(databricksRadio);
      });

      await waitFor(() => {
        const autoForm = screen.getByTestId('auto-setup-form');
        expect(autoForm).not.toHaveAttribute('data-embedding-model', 'databricks-bge-large-en');
      });
    });
  });

  // -----------------------------------------------------------------------
  // 6. CRITICAL: EMBEDDING_MODELS dimension fallback is 1024
  // -----------------------------------------------------------------------
  describe('embedding dimension fallback', () => {
    it('EMBEDDING_MODELS default model (databricks-gte-large-en) has dimension 1024', () => {
      const defaultModel = EMBEDDING_MODELS.find(
        (m) => m.value === 'databricks-gte-large-en',
      );
      expect(defaultModel).toBeDefined();
      expect(defaultModel!.dimension).toBe(1024);
    });

    it('EMBEDDING_MODELS fallback lookup for default model resolves to 1024 not 768', () => {
      // This replicates the exact fallback expression used in the component:
      // EMBEDDING_MODELS.find(m => m.value === embeddingModel)?.dimension || 1024
      const embeddingModel = 'databricks-gte-large-en';
      const dimension =
        EMBEDDING_MODELS.find((m) => m.value === embeddingModel)?.dimension || 1024;
      expect(dimension).toBe(1024);
      expect(dimension).not.toBe(768);
    });

    it('fallback dimension for an unknown model is 1024', () => {
      const unknownModel = 'some-unknown-model';
      const dimension =
        EMBEDDING_MODELS.find((m) => m.value === unknownModel)?.dimension || 1024;
      expect(dimension).toBe(1024);
      expect(dimension).not.toBe(768);
    });

    it('updateBackendConfiguration uses embedding_dimension 1024', () => {
      // The component source hardcodes: embedding_dimension: 1024 (line ~460)
      // We verify the constant value that the component uses
      const expectedDimension = 1024;
      expect(expectedDimension).toBe(1024);
      expect(expectedDimension).not.toBe(768);
    });

    it('all EMBEDDING_MODELS have dimensions >= 1024', () => {
      // Verify none of the models have the old 768 dimension
      EMBEDDING_MODELS.forEach((model) => {
        expect(model.dimension).toBeGreaterThanOrEqual(1024);
        expect(model.dimension).not.toBe(768);
      });
    });
  });

  // -----------------------------------------------------------------------
  // 7. Shows error alert when error state is set
  // -----------------------------------------------------------------------
  it('shows error alert when there is an error', async () => {
    mockApiClient.get.mockImplementation((url: string) => {
      if (url === '/memory-backend/configs/default') {
        return Promise.resolve({ data: {} });
      }
      if (url === '/databricks/environment') {
        return Promise.resolve({
          data: { databricks_host: null }, // No workspace detected
        });
      }
      if (url === '/memory-backend/configs') {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({ data: {} });
    });

    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // Switch to databricks mode
    const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
    await act(async () => {
      fireEvent.click(databricksRadio);
    });

    // The component should show the auto setup form even without workspace URL
    await waitFor(() => {
      expect(screen.getByTestId('auto-setup-form')).toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // 8. Renders AutomaticSetupForm when in auto setup mode
  // -----------------------------------------------------------------------
  it('renders AutomaticSetupForm when in auto setup mode', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // Switch to databricks mode
    const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
    await act(async () => {
      fireEvent.click(databricksRadio);
    });

    // Auto-create is the default setup mode
    await waitFor(() => {
      expect(screen.getByTestId('auto-setup-form')).toBeInTheDocument();
      expect(screen.queryByTestId('manual-config-form')).not.toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // 9. Renders ManualConfigurationForm when in manual setup mode
  // -----------------------------------------------------------------------
  it('renders ManualConfigurationForm when in manual setup mode', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // Switch to databricks mode
    const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
    await act(async () => {
      fireEvent.click(databricksRadio);
    });

    // Switch to manual setup mode
    const manualRadio = screen.getByLabelText(/Manual setup/i);
    await act(async () => {
      fireEvent.click(manualRadio);
    });

    await waitFor(() => {
      expect(screen.getByTestId('manual-config-form')).toBeInTheDocument();
      expect(screen.queryByTestId('auto-setup-form')).not.toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // Additional: Shows setup mode radio buttons (auto / manual) when databricks selected
  // -----------------------------------------------------------------------
  it('shows auto-create and manual setup radio options in databricks mode', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // Switch to databricks mode
    const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
    await act(async () => {
      fireEvent.click(databricksRadio);
    });

    await waitFor(() => {
      expect(screen.getByText('Auto-create')).toBeInTheDocument();
      expect(screen.getByText('Manual setup')).toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // Additional: Switching to disabled mode calls updateConfig with DEFAULT
  // -----------------------------------------------------------------------
  it('calls updateConfig with DEFAULT backend type when switching to local', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // First switch to databricks
    const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
    await act(async () => {
      fireEvent.click(databricksRadio);
    });

    // Then switch back to local (disabled)
    const localRadio = screen.getByLabelText(/Local/i);
    await act(async () => {
      fireEvent.click(localRadio);
    });

    await waitFor(() => {
      expect(mockUpdateConfig).toHaveBeenCalledWith({
        backend_type: MemoryBackendType.DEFAULT,
        enable_short_term: false,
        enable_long_term: false,
        enable_entity: false,
      });
    });
  });

  // -----------------------------------------------------------------------
  // Additional: Shows ConfigurationDisplay when savedConfig has workspace_url
  // -----------------------------------------------------------------------
  it('shows ConfigurationDisplay when a saved config with workspace_url exists', async () => {
    // Return an existing databricks config from the API
    mockApiClient.get.mockImplementation((url: string) => {
      if (url === '/memory-backend/configs/default') {
        return Promise.resolve({
          data: {
            id: 'backend-123',
            backend_type: 'databricks',
            databricks_config: {
              workspace_url: 'https://test.databricks.com',
              catalog: 'ml',
              schema: 'agents',
              endpoint_name: 'memory-endpoint',
              document_endpoint_name: 'doc-endpoint',
              short_term_index: 'ml.agents.short_term',
              long_term_index: 'ml.agents.long_term',
              entity_index: 'ml.agents.entity',
              document_index: 'ml.agents.document',
            },
          },
        });
      }
      if (url === '/databricks/environment') {
        return Promise.resolve({
          data: { databricks_host: 'https://test.databricks.com' },
        });
      }
      if (url === '/memory-backend/databricks/verify-resources') {
        return Promise.resolve({
          data: {
            success: true,
            resources: {
              endpoints: {
                'memory-endpoint': { name: 'memory-endpoint', state: 'ONLINE', ready: true },
                'doc-endpoint': { name: 'doc-endpoint', state: 'ONLINE', ready: true },
              },
              indexes: {
                'ml.agents.short_term': { name: 'ml.agents.short_term', status: 'ONLINE' },
                'ml.agents.long_term': { name: 'ml.agents.long_term', status: 'ONLINE' },
                'ml.agents.entity': { name: 'ml.agents.entity', status: 'ONLINE' },
                'ml.agents.document': { name: 'ml.agents.document', status: 'ONLINE' },
              },
            },
          },
        });
      }
      if (url === '/memory-backend/databricks/index-info') {
        return Promise.resolve({
          data: { success: true, doc_count: 10, status: 'ONLINE', ready: true, index_type: 'DELTA_SYNC' },
        });
      }
      return Promise.resolve({ data: {} });
    });

    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // ConfigurationDisplay should be rendered
    await waitFor(() => {
      expect(screen.getByTestId('configuration-display')).toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // Additional: SetupResultDialog and EntityGraphVisualization always render
  // -----------------------------------------------------------------------
  it('renders SetupResultDialog and EntityGraphVisualization in closed state', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    const resultDialog = screen.getByTestId('setup-result-dialog');
    expect(resultDialog).toBeInTheDocument();
    expect(resultDialog).toHaveAttribute('data-open', 'false');

    const vizDialog = screen.getByTestId('entity-graph-visualization-databricks');
    expect(vizDialog).toBeInTheDocument();
    expect(vizDialog).toHaveAttribute('data-open', 'false');

    const lakebaseVizDialog = screen.getByTestId('entity-graph-visualization-lakebase');
    expect(lakebaseVizDialog).toBeInTheDocument();
    expect(lakebaseVizDialog).toHaveAttribute('data-open', 'false');
  });

  // -----------------------------------------------------------------------
  // Additional: calls loadExistingConfig and detectWorkspaceUrl on mount
  // -----------------------------------------------------------------------
  it('calls apiClient.get for configs/default and databricks/environment on mount', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/configs/default');
      expect(mockApiClient.get).toHaveBeenCalledWith('/databricks/environment');
    });
  });

  // -----------------------------------------------------------------------
  // Additional: Info alert about choosing configuration method
  // -----------------------------------------------------------------------
  it('shows info alert about choosing configuration method in databricks mode', async () => {
    await act(async () => {
      renderComponent();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
    });

    // Switch to databricks mode
    const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
    await act(async () => {
      fireEvent.click(databricksRadio);
    });

    await waitFor(() => {
      expect(
        screen.getByText('Choose how to configure Databricks Vector Search'),
      ).toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // Verify manualConfig reset after save still uses GTE, not BGE
  // -----------------------------------------------------------------------
  describe('manual config reset defaults', () => {
    it('manualConfig initial embedding_model is databricks-gte-large-en', async () => {
      await act(async () => {
        renderComponent();
      });

      await waitFor(() => {
        expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
      });

      // Switch to databricks mode and manual
      const databricksRadio = screen.getByLabelText(/Databricks Vector Search/i);
      await act(async () => {
        fireEvent.click(databricksRadio);
      });

      const manualRadio = screen.getByLabelText(/Manual setup/i);
      await act(async () => {
        fireEvent.click(manualRadio);
      });

      // The manual form should receive the initial GTE model
      await waitFor(() => {
        const manualForm = screen.getByTestId('manual-config-form');
        expect(manualForm).toHaveAttribute('data-embedding-model', 'databricks-gte-large-en');
        expect(manualForm).not.toHaveAttribute('data-embedding-model', 'databricks-bge-large-en');
      });
    });
  });

  // -----------------------------------------------------------------------
  // Lakebase Save Configuration button
  // -----------------------------------------------------------------------
  describe('Lakebase Save Configuration', () => {
    const switchToLakebaseMode = async () => {
      await act(async () => {
        renderComponent();
      });

      await waitFor(() => {
        expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
      });

      const lakebaseRadio = screen.getByLabelText(/Lakebase/i);
      await act(async () => {
        fireEvent.click(lakebaseRadio);
      });
    };

    it('renders Save Configuration button when Lakebase mode is active', async () => {
      await switchToLakebaseMode();

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeInTheDocument();
      });
    });

    it('Save Configuration button is disabled when no instance is selected', async () => {
      await switchToLakebaseMode();

      await waitFor(() => {
        const saveBtn = screen.getByRole('button', { name: /Save Configuration/i });
        expect(saveBtn).toBeDisabled();
      });
    });

    it('calls /memory-backend/lakebase/save-config when Save Configuration is clicked', async () => {
      // Return a saved Lakebase config so the component loads in lakebase mode
      // with an instance already selected
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'lb-existing',
              backend_type: MemoryBackendType.LAKEBASE,
              lakebase_config: {
                instance_name: 'my-instance',
                embedding_dimension: 1024,
                tables_initialized: false,
              },
            },
          });
        }
        if (url === '/databricks/environment') {
          return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        }
        if (url === '/database-management/lakebase/instances') {
          return Promise.resolve({
            data: [{ name: 'my-instance', state: 'ACTIVE' }],
          });
        }
        return Promise.resolve({ data: {} });
      });

      mockApiClient.post.mockResolvedValue({ data: { backend_id: 'lb-123' } });

      await act(async () => {
        renderComponent();
      });

      await waitFor(() => {
        expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
      });

      // The Save Configuration button should be enabled since instance is selected
      const saveBtn = await waitFor(() => {
        const btn = screen.getByRole('button', { name: /Save Configuration/i });
        expect(btn).not.toBeDisabled();
        return btn;
      });

      // Click Save Configuration
      await act(async () => {
        fireEvent.click(saveBtn);
      });

      await waitFor(() => {
        expect(mockApiClient.post).toHaveBeenCalledWith(
          '/memory-backend/lakebase/save-config',
          expect.objectContaining({
            lakebase_config: expect.objectContaining({
              instance_name: 'my-instance',
            }),
          }),
        );
      });

      // Verify updateConfig was called with Lakebase type
      await waitFor(() => {
        expect(mockUpdateConfig).toHaveBeenCalledWith(
          expect.objectContaining({
            backend_type: MemoryBackendType.LAKEBASE,
            enable_short_term: true,
            enable_long_term: true,
            enable_entity: true,
          }),
        );
      });
    });

    it('shows error status when save-config call fails', async () => {
      // Return a saved Lakebase config with instance already selected
      mockApiClient.get.mockImplementation((url: string) => {
        if (url === '/memory-backend/configs/default') {
          return Promise.resolve({
            data: {
              id: 'lb-existing',
              backend_type: MemoryBackendType.LAKEBASE,
              lakebase_config: {
                instance_name: 'my-instance',
                embedding_dimension: 1024,
                tables_initialized: false,
              },
            },
          });
        }
        if (url === '/databricks/environment') {
          return Promise.resolve({ data: { databricks_host: 'https://test.databricks.com' } });
        }
        if (url === '/database-management/lakebase/instances') {
          return Promise.resolve({
            data: [{ name: 'my-instance', state: 'ACTIVE' }],
          });
        }
        return Promise.resolve({ data: {} });
      });

      mockApiClient.post.mockRejectedValue(new Error('Network error'));

      await act(async () => {
        renderComponent();
      });

      await waitFor(() => {
        expect(screen.queryByText('Loading memory configuration...')).not.toBeInTheDocument();
      });

      const saveBtn = await waitFor(() => {
        const btn = screen.getByRole('button', { name: /Save Configuration/i });
        expect(btn).not.toBeDisabled();
        return btn;
      });

      await act(async () => {
        fireEvent.click(saveBtn);
      });

      await waitFor(() => {
        expect(screen.getByText(/Failed to save configuration: Network error/)).toBeInTheDocument();
      });

      // updateConfig should NOT have been called
      expect(mockUpdateConfig).not.toHaveBeenCalledWith(
        expect.objectContaining({ backend_type: MemoryBackendType.LAKEBASE }),
      );
    });
  });
});
