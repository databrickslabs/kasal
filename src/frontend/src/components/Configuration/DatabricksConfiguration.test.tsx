import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { MemoryBackendType } from '../../types/memoryBackend';

// ---------------------------------------------------------------------------
// Hoisted mock references
// ---------------------------------------------------------------------------

const {
  mockGetInstance,
  mockGetDatabricksEnvironment,
  mockGetDatabricksConfig,
  mockSetDatabricksConfig,
  mockCheckPersonalTokenRequired,
  mockCheckDatabricksConnection,
  mockListTools,
  mockToggleToolEnabled,
  mockGetMemoryConfig,
  mockRefreshConfiguration,
  mockApiClientPost,
} = vi.hoisted(() => ({
  mockGetInstance: vi.fn(),
  mockGetDatabricksEnvironment: vi.fn(),
  mockGetDatabricksConfig: vi.fn(),
  mockSetDatabricksConfig: vi.fn(),
  mockCheckPersonalTokenRequired: vi.fn(),
  mockCheckDatabricksConnection: vi.fn(),
  mockListTools: vi.fn(),
  mockToggleToolEnabled: vi.fn(),
  mockGetMemoryConfig: vi.fn(),
  mockRefreshConfiguration: vi.fn(),
  mockApiClientPost: vi.fn(),
}));

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string }) => options?.defaultValue || key,
    i18n: { changeLanguage: vi.fn().mockResolvedValue(undefined) },
  }),
}));

vi.mock('../../api/DatabricksService', () => ({
  DatabricksService: {
    getInstance: mockGetInstance,
  },
}));

vi.mock('../../api/MemoryBackendService', () => ({
  MemoryBackendService: {
    getConfig: mockGetMemoryConfig,
  },
}));

vi.mock('../../api/ToolService', () => ({
  ToolService: {
    listTools: mockListTools,
    toggleToolEnabled: mockToggleToolEnabled,
  },
}));

vi.mock('../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: {
    getState: () => ({ refreshConfiguration: mockRefreshConfiguration }),
  },
}));

vi.mock('../../config/api/ApiConfig', () => ({
  default: { post: mockApiClientPost },
}));

// Import AFTER mocks so the component picks up the mocked modules.
import DatabricksConfiguration from './DatabricksConfiguration';

// ---------------------------------------------------------------------------
// Helpers / default mock data
// ---------------------------------------------------------------------------

const theme = createTheme();
const KNOWLEDGE_TOOL_ID = 36;

const baseConfig = {
  workspace_url: '',
  warehouse_id: 'wh-123',
  catalog: 'main',
  schema: 'default',
  enabled: true,
  mlflow_enabled: false,
  mlflow_experiment_name: 'kasal-crew-execution-traces',
  evaluation_enabled: false,
  evaluation_judge_model: '',
  volume_enabled: false,
  volume_path: 'main.default.task_outputs',
  volume_file_format: 'json' as const,
  volume_create_date_dirs: true,
  knowledge_volume_enabled: false,
  knowledge_volume_path: 'main.default.knowledge',
  knowledge_chunk_size: 1000,
  knowledge_chunk_overlap: 200,
};

const databricksMemoryConfig = {
  backend_type: MemoryBackendType.DATABRICKS,
  databricks_config: { endpoint_name: 'ep-1', memory_index: 'idx-1' },
};

const lakebaseMemoryConfig = {
  backend_type: MemoryBackendType.LAKEBASE,
  lakebase_config: { memory_table: 'crew_memory', embedding_dimension: 1024 },
};

const setupServiceDefaults = () => {
  mockGetDatabricksEnvironment.mockResolvedValue({ databricks_host: 'https://example.com' });
  mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig });
  mockSetDatabricksConfig.mockImplementation((c) => Promise.resolve({ ...c }));
  mockCheckPersonalTokenRequired.mockResolvedValue({ personal_token_required: false, message: '' });
  mockCheckDatabricksConnection.mockResolvedValue({ status: 'ok', message: 'Connected', connected: true });
  mockListTools.mockResolvedValue([{ id: KNOWLEDGE_TOOL_ID, enabled: false }]);
  mockToggleToolEnabled.mockResolvedValue({ enabled: true });
  mockGetMemoryConfig.mockResolvedValue(lakebaseMemoryConfig);

  mockGetInstance.mockReturnValue({
    getDatabricksEnvironment: mockGetDatabricksEnvironment,
    getDatabricksConfig: mockGetDatabricksConfig,
    setDatabricksConfig: mockSetDatabricksConfig,
    checkPersonalTokenRequired: mockCheckPersonalTokenRequired,
    checkDatabricksConnection: mockCheckDatabricksConnection,
  });
};

const renderComponent = async (props: { onSaved?: () => void } = {}) => {
  await act(async () => {
    render(
      <ThemeProvider theme={theme}>
        <DatabricksConfiguration {...props} />
      </ThemeProvider>
    );
  });
};

beforeEach(() => {
  vi.clearAllMocks();
  setupServiceDefaults();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('DatabricksConfiguration', () => {
  describe('initial load', () => {
    it('shows a loading indicator before initial load completes', () => {
      // Keep the config promise pending so initialLoading stays true.
      mockGetDatabricksConfig.mockReturnValue(new Promise(() => {}));
      mockGetMemoryConfig.mockReturnValue(new Promise(() => {}));
      render(
        <ThemeProvider theme={theme}>
          <DatabricksConfiguration />
        </ThemeProvider>
      );
      expect(screen.getByText('Loading Databricks configuration...')).toBeInTheDocument();
    });

    it('loads saved config and renders the form fields', async () => {
      await renderComponent();
      await waitFor(() => {
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument();
      });
      // warehouse_id value from loaded config.
      expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument();
      expect(mockGetDatabricksConfig).toHaveBeenCalled();
      expect(mockGetDatabricksEnvironment).toHaveBeenCalled();
    });

    it('populates workspace URL field from backend environment', async () => {
      await renderComponent();
      await waitFor(() => {
        expect(screen.getByDisplayValue('https://example.com')).toBeInTheDocument();
      });
    });

    it('checks token status on load when config is enabled', async () => {
      await renderComponent();
      await waitFor(() => expect(mockCheckPersonalTokenRequired).toHaveBeenCalled());
    });

    it('does not check token status on load when config is disabled', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, enabled: false });
      await renderComponent();
      await waitFor(() => expect(mockGetDatabricksConfig).toHaveBeenCalled());
      expect(mockCheckPersonalTokenRequired).not.toHaveBeenCalled();
    });

    it('syncs the knowledge tool on load when state does not match', async () => {
      // Tool is disabled, but enabled+knowledge_volume_enabled+memory configured -> should enable.
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, knowledge_volume_enabled: true });
      mockListTools.mockResolvedValue([{ id: KNOWLEDGE_TOOL_ID, enabled: false }]);
      await renderComponent();
      await waitFor(() => expect(mockToggleToolEnabled).toHaveBeenCalledWith(KNOWLEDGE_TOOL_ID));
    });

    it('does not toggle the knowledge tool on load when state already matches', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, knowledge_volume_enabled: false });
      mockListTools.mockResolvedValue([{ id: KNOWLEDGE_TOOL_ID, enabled: false }]);
      await renderComponent();
      await waitFor(() => expect(mockListTools).toHaveBeenCalled());
      expect(mockToggleToolEnabled).not.toHaveBeenCalled();
    });

    it('handles environment fetch error gracefully', async () => {
      mockGetDatabricksEnvironment.mockRejectedValue(new Error('env fail'));
      await renderComponent();
      await waitFor(() => {
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument();
      });
      // Falls back to config.workspace_url (empty) without crashing.
      expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument();
    });

    it('handles config load error gracefully', async () => {
      mockGetDatabricksConfig.mockRejectedValue(new Error('load fail'));
      await renderComponent();
      await waitFor(() => {
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument();
      });
    });

    it('handles tool-sync error on load gracefully', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, knowledge_volume_enabled: true });
      mockListTools.mockRejectedValue(new Error('tools fail'));
      await renderComponent();
      await waitFor(() => {
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument();
      });
    });

    it('returns null config from getDatabricksConfig without crashing', async () => {
      mockGetDatabricksConfig.mockResolvedValue(null);
      await renderComponent();
      await waitFor(() => {
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument();
      });
    });
  });

  describe('memory backend configuration check', () => {
    it('marks memory backend configured for a valid lakebase config', async () => {
      mockGetMemoryConfig.mockResolvedValue(lakebaseMemoryConfig);
      await renderComponent();
      await waitFor(() => {
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument();
      });
      // Lakebase pgvector stores knowledge embeddings, so the warning should NOT appear.
      expect(screen.queryByText(/Requires a Lakebase memory backend/)).not.toBeInTheDocument();
    });

    it('marks memory backend unconfigured for a databricks (Vector Search) backend', async () => {
      // Vector Search has been removed; knowledge sources need Lakebase pgvector.
      mockGetMemoryConfig.mockResolvedValue(databricksMemoryConfig);
      await renderComponent();
      await waitFor(() => {
        expect(screen.getByText(/Requires a Lakebase memory backend/)).toBeInTheDocument();
      });
    });

    it('marks memory backend unconfigured for a default (ChromaDB) backend', async () => {
      mockGetMemoryConfig.mockResolvedValue({ backend_type: MemoryBackendType.DEFAULT });
      await renderComponent();
      await waitFor(() => {
        expect(screen.getByText(/Requires a Lakebase memory backend/)).toBeInTheDocument();
      });
    });

    it('handles getConfig returning null', async () => {
      mockGetMemoryConfig.mockResolvedValue(null);
      await renderComponent();
      await waitFor(() => {
        expect(screen.getByText(/Requires a Lakebase memory backend/)).toBeInTheDocument();
      });
    });

    it('handles getConfig error gracefully', async () => {
      mockGetMemoryConfig.mockRejectedValue(new Error('mem fail'));
      await renderComponent();
      await waitFor(() => {
        expect(screen.getByText(/Requires a Lakebase memory backend/)).toBeInTheDocument();
      });
    });
  });

  describe('token status alert', () => {
    it('renders the personal token warning when required', async () => {
      mockCheckPersonalTokenRequired.mockResolvedValue({
        personal_token_required: true,
        message: 'Personal token required',
      });
      await renderComponent();
      await waitFor(() => {
        expect(screen.getByText('Personal token required')).toBeInTheDocument();
      });
    });
  });

  describe('toggles', () => {
    it('toggles Databricks enabled off and clears token status', async () => {
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      // The first switch is the main "enabled" toggle.
      const switches = screen.getAllByRole('checkbox');
      const enabledSwitch = switches[0];
      fireEvent.click(enabledSwitch); // turn off
      await waitFor(() => expect(enabledSwitch).not.toBeChecked());
    });

    it('toggles Databricks enabled on and checks token status', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, enabled: false });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      mockCheckPersonalTokenRequired.mockClear();
      const enabledSwitch = screen.getAllByRole('checkbox')[0];
      fireEvent.click(enabledSwitch); // turn on
      await waitFor(() => expect(mockCheckPersonalTokenRequired).toHaveBeenCalled());
    });

    it('handles error when checking token status during enable toggle', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, enabled: false });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      mockCheckPersonalTokenRequired.mockRejectedValueOnce(new Error('token fail'));
      const enabledSwitch = screen.getAllByRole('checkbox')[0];
      fireEvent.click(enabledSwitch);
      // Should not crash.
      await waitFor(() => expect(enabledSwitch).toBeChecked());
    });

    it('toggles MLflow on and persists via apiClient, revealing experiment name field', async () => {
      mockApiClientPost.mockResolvedValue({ data: {} });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      // MLflow switch is the second checkbox.
      const mlflowSwitch = screen.getAllByRole('checkbox')[1];
      fireEvent.click(mlflowSwitch);
      await waitFor(() =>
        expect(mockApiClientPost).toHaveBeenCalledWith('/mlflow/status', { enabled: true })
      );
      // Experiment name TextField now visible.
      await waitFor(() =>
        expect(screen.getByDisplayValue('kasal-crew-execution-traces')).toBeInTheDocument()
      );
    });

    it('shows a saveFirst error when MLflow persist returns 404', async () => {
      mockApiClientPost.mockRejectedValue({ response: { status: 404 } });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const mlflowSwitch = screen.getAllByRole('checkbox')[1];
      fireEvent.click(mlflowSwitch);
      await waitFor(() =>
        expect(
          screen.getByText('Please save Databricks settings first to persist MLflow.')
        ).toBeInTheDocument()
      );
    });

    it('handles non-404 MLflow persist errors without notification', async () => {
      mockApiClientPost.mockRejectedValue({ response: { status: 500 } });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const mlflowSwitch = screen.getAllByRole('checkbox')[1];
      fireEvent.click(mlflowSwitch);
      await waitFor(() => expect(mockApiClientPost).toHaveBeenCalled());
      expect(
        screen.queryByText('Please save Databricks settings first to persist MLflow.')
      ).not.toBeInTheDocument();
    });

    it('toggles evaluation on with apiClient persist and shows judge model field', async () => {
      mockApiClientPost.mockResolvedValue({ data: {} });
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, mlflow_enabled: true });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      // Find the evaluation switch by locating the MLflow Evaluation label region.
      // After mlflow enabled, evaluation switch is present; toggle it.
      const evalLabel = screen.getByText('MLflow Evaluation');
      const evalSwitch = evalLabel.parentElement?.querySelector('input[type="checkbox"]') as HTMLElement;
      fireEvent.click(evalSwitch);
      await waitFor(() =>
        expect(mockApiClientPost).toHaveBeenCalledWith('/mlflow/evaluation-status', { enabled: true })
      );
      // Judge model placeholder field appears.
      await waitFor(() =>
        expect(screen.getByPlaceholderText('databricks:/your-judge-endpoint')).toBeInTheDocument()
      );
    });

    it('shows a saveFirst error when evaluation persist returns 404', async () => {
      mockApiClientPost.mockRejectedValue({ response: { status: 404 } });
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, mlflow_enabled: true });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const evalLabel = screen.getByText('MLflow Evaluation');
      const evalSwitch = evalLabel.parentElement?.querySelector('input[type="checkbox"]') as HTMLElement;
      fireEvent.click(evalSwitch);
      await waitFor(() =>
        expect(
          screen.getByText('Please save Databricks settings first to persist MLflow Evaluation.')
        ).toBeInTheDocument()
      );
    });

    it('handles non-404 evaluation persist errors without notification', async () => {
      mockApiClientPost.mockRejectedValue({ response: { status: 500 } });
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, mlflow_enabled: true });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const evalLabel = screen.getByText('MLflow Evaluation');
      const evalSwitch = evalLabel.parentElement?.querySelector('input[type="checkbox"]') as HTMLElement;
      fireEvent.click(evalSwitch);
      await waitFor(() => expect(mockApiClientPost).toHaveBeenCalled());
      expect(
        screen.queryByText('Please save Databricks settings first to persist MLflow Evaluation.')
      ).not.toBeInTheDocument();
    });

    it('toggles the volume upload switch and shows the example path', async () => {
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const volumeEnableLabel = screen.getByText('Enable Volume Uploads for All Tasks');
      const volumeSwitch = volumeEnableLabel
        .closest('label')
        ?.querySelector('input[type="checkbox"]') as HTMLElement;
      fireEvent.click(volumeSwitch);
      await waitFor(() => expect(screen.getByText('Example output path:')).toBeInTheDocument());
    });

    it('toggles the date dirs switch', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, volume_enabled: true });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const dateDirsLabel = screen.getByText('Create Date-based Directories');
      const dateDirsSwitch = dateDirsLabel
        .closest('label')
        ?.querySelector('input[type="checkbox"]') as HTMLElement;
      // It starts checked (volume_create_date_dirs true). Toggle off.
      fireEvent.click(dateDirsSwitch);
      await waitFor(() => expect(dateDirsSwitch).not.toBeChecked());
    });

    it('enables the knowledge volume switch when memory backend is configured', async () => {
      mockGetMemoryConfig.mockResolvedValue(lakebaseMemoryConfig);
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const knowledgeLabel = screen.getByText('Enable Knowledge Source Volume');
      const knowledgeSwitch = knowledgeLabel
        .closest('label')
        ?.querySelector('input[type="checkbox"]') as HTMLElement;
      expect(knowledgeSwitch).not.toBeDisabled();
      fireEvent.click(knowledgeSwitch);
      await waitFor(() => expect(screen.getByText(/Knowledge files will be organized as:/)).toBeInTheDocument());
    });
  });

  describe('form field input handlers', () => {
    it('updates the warehouse id field', async () => {
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const input = screen.getByDisplayValue('wh-123');
      fireEvent.change(input, { target: { value: 'wh-999' } });
      expect(screen.getByDisplayValue('wh-999')).toBeInTheDocument();
    });

    it('updates the volume path field', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, volume_enabled: true });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const input = screen.getByDisplayValue('main.default.task_outputs');
      fireEvent.change(input, { target: { value: 'main.default.outs' } });
      expect(screen.getByDisplayValue('main.default.outs')).toBeInTheDocument();
    });

    it('updates the knowledge chunk size field with a parsed integer', async () => {
      mockGetDatabricksConfig.mockResolvedValue({
        ...baseConfig,
        knowledge_volume_enabled: true,
      });
      mockGetMemoryConfig.mockResolvedValue(lakebaseMemoryConfig);
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const input = screen.getByDisplayValue('1000');
      fireEvent.change(input, { target: { value: '2000' } });
      expect(screen.getByDisplayValue('2000')).toBeInTheDocument();
    });

    it('falls back to default chunk size on non-numeric input', async () => {
      mockGetDatabricksConfig.mockResolvedValue({
        ...baseConfig,
        knowledge_volume_enabled: true,
      });
      mockGetMemoryConfig.mockResolvedValue(lakebaseMemoryConfig);
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      const input = screen.getByDisplayValue('1000');
      fireEvent.change(input, { target: { value: 'abc' } });
      // parseInt('abc') is NaN -> falls back to 1000.
      expect(screen.getByDisplayValue('1000')).toBeInTheDocument();
    });
  });

  describe('save', () => {
    it('saves successfully and fires onSaved + refresh + custom event', async () => {
      const onSaved = vi.fn();
      const eventSpy = vi.spyOn(window, 'dispatchEvent');
      await renderComponent({ onSaved });
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      mockToggleToolEnabled.mockClear();
      fireEvent.click(screen.getByRole('button', { name: /save/i }));

      await waitFor(() => expect(mockSetDatabricksConfig).toHaveBeenCalled());
      await waitFor(() =>
        expect(screen.getByText('Databricks configuration saved successfully')).toBeInTheDocument()
      );
      expect(onSaved).toHaveBeenCalled();
      expect(mockRefreshConfiguration).toHaveBeenCalled();
      expect(eventSpy).toHaveBeenCalledWith(expect.any(CustomEvent));
      eventSpy.mockRestore();
    });

    it('validates required fields and blocks save when enabled with empty fields', async () => {
      mockGetDatabricksConfig.mockResolvedValue({
        ...baseConfig,
        warehouse_id: '',
        catalog: '',
        schema: '',
      });
      await renderComponent();
      await waitFor(() =>
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument()
      );

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() =>
        expect(
          screen.getByText(/Please fill in all required fields: Warehouse ID, Catalog, Schema/)
        ).toBeInTheDocument()
      );
      expect(mockSetDatabricksConfig).not.toHaveBeenCalled();
    });

    it('saves without validation when Databricks is disabled', async () => {
      mockGetDatabricksConfig.mockResolvedValue({
        ...baseConfig,
        enabled: false,
        warehouse_id: '',
        catalog: '',
        schema: '',
      });
      mockSetDatabricksConfig.mockResolvedValue({
        ...baseConfig,
        enabled: false,
        warehouse_id: '',
        catalog: '',
        schema: '',
      });
      await renderComponent();
      await waitFor(() =>
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument()
      );

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(mockSetDatabricksConfig).toHaveBeenCalled());
      // tokenStatus cleared since disabled -> checkPersonalTokenRequired not called for after-save branch.
    });

    it('shows an error notification when save fails', async () => {
      mockSetDatabricksConfig.mockRejectedValue(new Error('save boom'));
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(screen.getByText('save boom')).toBeInTheDocument());
    });

    it('shows a generic error message when save rejects with a non-Error', async () => {
      mockSetDatabricksConfig.mockRejectedValue('weird');
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() =>
        expect(screen.getByText('Failed to save Databricks configuration')).toBeInTheDocument()
      );
    });

    it('continues save when tool sync fails', async () => {
      mockListTools.mockResolvedValueOnce([{ id: KNOWLEDGE_TOOL_ID, enabled: false }]); // load
      mockListTools.mockRejectedValueOnce(new Error('tool list fail')); // save
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() =>
        expect(screen.getByText('Databricks configuration saved successfully')).toBeInTheDocument()
      );
    });

    it('toggles tool on save when knowledge volume enabled and memory configured', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, knowledge_volume_enabled: true });
      mockSetDatabricksConfig.mockResolvedValue({ ...baseConfig, knowledge_volume_enabled: true });
      mockGetMemoryConfig.mockResolvedValue(lakebaseMemoryConfig);
      // load sees tool already enabled (matches), save expects enabled too -> but make it mismatch on save.
      mockListTools.mockResolvedValue([{ id: KNOWLEDGE_TOOL_ID, enabled: false }]);
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      mockToggleToolEnabled.mockClear();
      fireEvent.click(screen.getByRole('button', { name: /save/i }));
      await waitFor(() => expect(mockToggleToolEnabled).toHaveBeenCalledWith(KNOWLEDGE_TOOL_ID));
    });
  });

  describe('check connection', () => {
    it('checks connection successfully and renders a success alert', async () => {
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /check connection/i }));
      await waitFor(() => expect(screen.getByText('Connected')).toBeInTheDocument());
    });

    it('renders an error alert when connection is not connected', async () => {
      mockCheckDatabricksConnection.mockResolvedValue({
        status: 'fail',
        message: 'Cannot connect',
        connected: false,
      });
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /check connection/i }));
      await waitFor(() => expect(screen.getByText('Cannot connect')).toBeInTheDocument());
    });

    it('shows an error notification when connection check throws', async () => {
      mockCheckDatabricksConnection.mockRejectedValue(new Error('conn boom'));
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /check connection/i }));
      await waitFor(() => expect(screen.getByText('conn boom')).toBeInTheDocument());
    });

    it('shows a generic error when connection check rejects with a non-Error', async () => {
      mockCheckDatabricksConnection.mockRejectedValue('weird');
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /check connection/i }));
      await waitFor(() =>
        expect(screen.getByText('Failed to check Databricks connection')).toBeInTheDocument()
      );
    });

    it('disables the check connection button when Databricks is disabled', async () => {
      mockGetDatabricksConfig.mockResolvedValue({ ...baseConfig, enabled: false });
      await renderComponent();
      await waitFor(() =>
        expect(screen.queryByText('Loading Databricks configuration...')).not.toBeInTheDocument()
      );
      expect(screen.getByRole('button', { name: /check connection/i })).toBeDisabled();
    });
  });

  describe('notification close', () => {
    it('closes the snackbar notification', async () => {
      mockCheckDatabricksConnection.mockRejectedValue(new Error('conn boom'));
      await renderComponent();
      await waitFor(() => expect(screen.getByDisplayValue('wh-123')).toBeInTheDocument());

      fireEvent.click(screen.getByRole('button', { name: /check connection/i }));
      await waitFor(() => expect(screen.getByText('conn boom')).toBeInTheDocument());

      // The Alert has a close button inside the Snackbar.
      const closeButtons = screen.getAllByRole('button', { name: /close/i });
      fireEvent.click(closeButtons[0]);
      await waitFor(() => expect(screen.queryByText('conn boom')).not.toBeInTheDocument());
    });
  });
});
