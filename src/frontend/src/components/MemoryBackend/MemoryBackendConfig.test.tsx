import { vi, beforeEach, describe, it, expect, type Mock } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { MemoryBackendConfig } from './MemoryBackendConfig';
import { MemoryBackendType } from '../../types/memoryBackend';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

const mockUpdateConfig = vi.fn();
const mockUpdateDatabricksConfig = vi.fn();
const mockTestDatabricksConnection = vi.fn();
const mockLoadAvailableIndexes = vi.fn();
const mockValidateConfig = vi.fn().mockResolvedValue(true);
const mockClearError = vi.fn();

const defaultStoreState = {
  config: {
    backend_type: 'default' as const,
    enable_short_term: true,
    enable_long_term: true,
    enable_entity: true,
  },
  error: null as string | null,
  connectionTestResult: null,
  isTestingConnection: false,
  availableIndexes: [] as Array<{ name: string; dimension: number }>,
  isLoadingIndexes: false,
  validationErrors: [] as string[],
  updateConfig: mockUpdateConfig,
  updateDatabricksConfig: mockUpdateDatabricksConfig,
  testDatabricksConnection: mockTestDatabricksConnection,
  loadAvailableIndexes: mockLoadAvailableIndexes,
  validateConfig: mockValidateConfig,
  clearError: mockClearError,
};

let storeOverrides: Partial<typeof defaultStoreState> = {};

vi.mock('../../store/memoryBackend', () => ({
  useMemoryBackendStore: () => ({
    ...defaultStoreState,
    ...storeOverrides,
  }),
}));

const mockSetDefaultConfig = vi.fn();
vi.mock('../../api/DefaultMemoryBackendService', () => ({
  DefaultMemoryBackendService: {
    getInstance: () => ({
      setDefaultConfig: mockSetDefaultConfig,
    }),
  },
}));

vi.mock('../../api/MemoryBackendService', () => ({
  MemoryBackendService: {
    validateConfig: vi.fn().mockResolvedValue({ valid: true, errors: [] }),
    testDatabricksConnection: vi.fn().mockResolvedValue({ success: true, message: 'ok' }),
    getAvailableDatabricksIndexes: vi.fn().mockResolvedValue({ indexes: [] }),
    createDatabricksIndex: vi.fn().mockResolvedValue({ success: true, message: 'created' }),
  },
}));

vi.mock('../../types/memoryBackend', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../types/memoryBackend')>();
  return {
    ...actual,
    getBackendDisplayName: vi.fn((type: string) => {
      if (type === 'default') return 'Default (ChromaDB + SQLite)';
      if (type === 'databricks') return 'Databricks Vector Search';
      return type;
    }),
    getBackendDescription: vi.fn((type: string) => {
      if (type === 'default') return 'Uses CrewAI built-in memory storage.';
      if (type === 'databricks') return 'Uses Databricks Vector Search.';
      return '';
    }),
  };
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const theme = createTheme();

function renderComponent(props: { embedded?: boolean; onConfigChange?: (valid: boolean) => void } = {}) {
  return render(
    <ThemeProvider theme={theme}>
      <MemoryBackendConfig {...props} />
    </ThemeProvider>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('MemoryBackendConfig', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    storeOverrides = {};
  });

  // -----------------------------------------------------------------------
  // 1. Title rendering
  // -----------------------------------------------------------------------
  describe('rendering in default (non-embedded) mode', () => {
    it('renders with title "Memory Backend Configuration"', () => {
      renderComponent();
      expect(screen.getByText('Memory Backend Configuration')).toBeInTheDocument();
    });

    it('does not render the embedded description text', () => {
      renderComponent();
      expect(
        screen.queryByText(/Configure the default memory storage backend/),
      ).not.toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // 2. Embedded / compact mode
  // -----------------------------------------------------------------------
  describe('rendering in embedded mode', () => {
    it('renders compact version with embedded heading', () => {
      renderComponent({ embedded: true });
      expect(screen.getByText('Default Memory Backend for All Agents')).toBeInTheDocument();
      expect(
        screen.getByText(/Configure the default memory storage backend/),
      ).toBeInTheDocument();
    });

    it('does not render the non-embedded title', () => {
      renderComponent({ embedded: true });
      expect(screen.queryByText('Memory Backend Configuration')).not.toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // 3. Error alert
  // -----------------------------------------------------------------------
  describe('error display', () => {
    it('shows error alert when store has an error', () => {
      storeOverrides = { error: 'Something went wrong' };
      renderComponent();
      expect(screen.getByText('Something went wrong')).toBeInTheDocument();
    });

    it('does not show error alert when error is null', () => {
      storeOverrides = { error: null };
      renderComponent();
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    });

    it('calls clearError when the error alert close button is clicked', () => {
      storeOverrides = { error: 'Connection failed' };
      renderComponent();
      const closeButton = screen.getByRole('button', { name: /close/i });
      fireEvent.click(closeButton);
      expect(mockClearError).toHaveBeenCalledTimes(1);
    });
  });

  // -----------------------------------------------------------------------
  // 4. Validation errors
  // -----------------------------------------------------------------------
  describe('validation errors', () => {
    it('shows validation errors when present', () => {
      storeOverrides = {
        validationErrors: ['Endpoint is required', 'Short-term index is required'],
      };
      renderComponent();
      expect(screen.getByText('Please fix the following errors:')).toBeInTheDocument();
      expect(screen.getByText('Endpoint is required')).toBeInTheDocument();
      expect(screen.getByText('Short-term index is required')).toBeInTheDocument();
    });

    it('does not show validation errors section when array is empty', () => {
      storeOverrides = { validationErrors: [] };
      renderComponent();
      expect(screen.queryByText('Please fix the following errors:')).not.toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // 5. handleBackendTypeChange - CRITICAL: embedding_dimension 1024
  // -----------------------------------------------------------------------
  describe('handleBackendTypeChange', () => {
    it('calls updateConfig with embedding_dimension: 1024 when switching to databricks', () => {
      renderComponent();

      // Find the Databricks radio button and click it
      const databricksRadio = screen.getByRole('radio', {
        name: /Databricks Vector Search/i,
      });
      fireEvent.click(databricksRadio);

      expect(mockUpdateConfig).toHaveBeenCalledTimes(1);
      expect(mockUpdateConfig).toHaveBeenCalledWith({
        backend_type: MemoryBackendType.DATABRICKS,
        databricks_config: {
          endpoint_name: '',
          short_term_index: '',
          embedding_dimension: 1024,
          auth_type: 'default',
        },
      });
    });

    it('calls updateConfig with databricks_config undefined when switching to default', () => {
      storeOverrides = {
        config: {
          backend_type: 'databricks' as MemoryBackendType,
          enable_short_term: true,
          enable_long_term: true,
          enable_entity: true,
          databricks_config: {
            endpoint_name: 'my-endpoint',
            short_term_index: 'my-index',
            embedding_dimension: 1024,
            auth_type: 'default' as const,
          },
        },
      };
      renderComponent();

      const defaultRadio = screen.getByRole('radio', {
        name: /Default \(ChromaDB \+ SQLite\)/i,
      });
      fireEvent.click(defaultRadio);

      expect(mockUpdateConfig).toHaveBeenCalledTimes(1);
      expect(mockUpdateConfig).toHaveBeenCalledWith({
        backend_type: MemoryBackendType.DEFAULT,
        databricks_config: undefined,
      });
    });

    it('sets exactly 1024, not 768 or any other value, for embedding_dimension', () => {
      renderComponent();

      const databricksRadio = screen.getByRole('radio', {
        name: /Databricks Vector Search/i,
      });
      fireEvent.click(databricksRadio);

      const calledWith = mockUpdateConfig.mock.calls[0][0];
      expect(calledWith.databricks_config.embedding_dimension).toBe(1024);
      expect(calledWith.databricks_config.embedding_dimension).not.toBe(768);
    });
  });

  // -----------------------------------------------------------------------
  // 6. Fallback databricks config uses 1024 dimension
  // -----------------------------------------------------------------------
  describe('databricks config fallback values', () => {
    it('renders Databricks section with fallback embedding_dimension of 1024 when databricks_config is undefined', async () => {
      storeOverrides = {
        config: {
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: true,
          enable_long_term: true,
          enable_entity: true,
          // databricks_config intentionally omitted to trigger fallback
        },
      };
      renderComponent();

      // The "Advanced Settings" section contains the Embedding Dimension field.
      // We need to expand it first.
      const advancedSettingsHeader = screen.getByText('Advanced Settings');
      fireEvent.click(advancedSettingsHeader);

      await waitFor(() => {
        const embeddingInput = screen.getByLabelText('Embedding Dimension') as HTMLInputElement;
        expect(embeddingInput.value).toBe('1024');
      });
    });

    it('renders Databricks section with existing embedding_dimension when databricks_config is present', async () => {
      storeOverrides = {
        config: {
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: true,
          enable_long_term: true,
          enable_entity: true,
          databricks_config: {
            endpoint_name: 'test-endpoint',
            short_term_index: 'test-index',
            embedding_dimension: 2048,
            auth_type: 'default' as const,
          },
        },
      };
      renderComponent();

      const advancedSettingsHeader = screen.getByText('Advanced Settings');
      fireEvent.click(advancedSettingsHeader);

      await waitFor(() => {
        const embeddingInput = screen.getByLabelText('Embedding Dimension') as HTMLInputElement;
        expect(embeddingInput.value).toBe('2048');
      });
    });

    it('falls back to 1024 on Embedding Dimension input when parsing produces NaN', async () => {
      storeOverrides = {
        config: {
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: true,
          enable_long_term: true,
          enable_entity: true,
          databricks_config: {
            endpoint_name: 'ep',
            short_term_index: 'idx',
            embedding_dimension: 1024,
            auth_type: 'default' as const,
          },
        },
      };
      renderComponent();

      const advancedSettingsHeader = screen.getByText('Advanced Settings');
      fireEvent.click(advancedSettingsHeader);

      await waitFor(() => {
        expect(screen.getByLabelText('Embedding Dimension')).toBeInTheDocument();
      });

      const embeddingInput = screen.getByLabelText('Embedding Dimension') as HTMLInputElement;
      // Clear the field to empty string - parseInt('') returns NaN, so fallback to 1024
      fireEvent.change(embeddingInput, { target: { value: '' } });

      expect(mockUpdateDatabricksConfig).toHaveBeenCalledWith({
        embedding_dimension: 1024,
      });
    });
  });

  // -----------------------------------------------------------------------
  // 7. Memory type toggle switches
  // -----------------------------------------------------------------------
  describe('memory type toggle switches', () => {
    it('renders Short-term Memory switch', () => {
      renderComponent();
      expect(screen.getByLabelText('Short-term Memory')).toBeInTheDocument();
    });

    it('renders Long-term Memory switch', () => {
      renderComponent();
      expect(screen.getByLabelText('Long-term Memory')).toBeInTheDocument();
    });

    it('renders Entity Memory switch', () => {
      renderComponent();
      expect(screen.getByLabelText('Entity Memory')).toBeInTheDocument();
    });

    it('all switches are checked by default', () => {
      renderComponent();
      const shortTerm = screen.getByLabelText('Short-term Memory') as HTMLInputElement;
      const longTerm = screen.getByLabelText('Long-term Memory') as HTMLInputElement;
      const entity = screen.getByLabelText('Entity Memory') as HTMLInputElement;

      expect(shortTerm.checked).toBe(true);
      expect(longTerm.checked).toBe(true);
      expect(entity.checked).toBe(true);
    });

    it('calls updateConfig when Short-term Memory switch is toggled off', () => {
      renderComponent();
      const shortTerm = screen.getByLabelText('Short-term Memory');
      fireEvent.click(shortTerm);
      expect(mockUpdateConfig).toHaveBeenCalledWith({ enable_short_term: false });
    });

    it('calls updateConfig when Long-term Memory switch is toggled off', () => {
      renderComponent();
      const longTerm = screen.getByLabelText('Long-term Memory');
      fireEvent.click(longTerm);
      expect(mockUpdateConfig).toHaveBeenCalledWith({ enable_long_term: false });
    });

    it('calls updateConfig when Entity Memory switch is toggled off', () => {
      renderComponent();
      const entity = screen.getByLabelText('Entity Memory');
      fireEvent.click(entity);
      expect(mockUpdateConfig).toHaveBeenCalledWith({ enable_entity: false });
    });
  });

  // -----------------------------------------------------------------------
  // 8. Save as Default button
  // -----------------------------------------------------------------------
  describe('"Save as Default" button', () => {
    it('shows "Save as Default" button when embedded=true', () => {
      renderComponent({ embedded: true });
      expect(screen.getByRole('button', { name: /Save as Default/i })).toBeInTheDocument();
    });

    it('does not show "Save as Default" button when not embedded', () => {
      renderComponent({ embedded: false });
      expect(screen.queryByRole('button', { name: /Save as Default/i })).not.toBeInTheDocument();
    });

    it('calls DefaultMemoryBackendService.setDefaultConfig when clicked', () => {
      renderComponent({ embedded: true });
      const saveButton = screen.getByRole('button', { name: /Save as Default/i });
      fireEvent.click(saveButton);
      expect(mockSetDefaultConfig).toHaveBeenCalledTimes(1);
      expect(mockSetDefaultConfig).toHaveBeenCalledWith(defaultStoreState.config);
    });

    it('disables Save as Default when there are validation errors', () => {
      storeOverrides = { validationErrors: ['Some error'] };
      renderComponent({ embedded: true });
      const saveButton = screen.getByRole('button', { name: /Save as Default/i });
      expect(saveButton).toBeDisabled();
    });
  });

  // -----------------------------------------------------------------------
  // Backend type radio buttons rendering
  // -----------------------------------------------------------------------
  describe('backend type radio buttons', () => {
    it('renders radio options for all MemoryBackendType values', () => {
      renderComponent();
      expect(screen.getByText('Default (ChromaDB + SQLite)')).toBeInTheDocument();
      expect(screen.getByText('Databricks Vector Search')).toBeInTheDocument();
    });

    it('has the default backend type selected initially', () => {
      renderComponent();
      const defaultRadio = screen.getByRole('radio', {
        name: /Default \(ChromaDB \+ SQLite\)/i,
      });
      expect(defaultRadio).toBeChecked();
    });
  });

  // -----------------------------------------------------------------------
  // Databricks-specific UI sections
  // -----------------------------------------------------------------------
  describe('Databricks configuration section', () => {
    beforeEach(() => {
      storeOverrides = {
        config: {
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: true,
          enable_long_term: true,
          enable_entity: true,
          databricks_config: {
            endpoint_name: '',
            short_term_index: '',
            embedding_dimension: 1024,
            auth_type: 'default' as const,
          },
        },
      };
    });

    it('renders Databricks Vector Search Configuration heading', () => {
      renderComponent();
      expect(
        screen.getByText('Databricks Vector Search Configuration'),
      ).toBeInTheDocument();
    });

    it('renders the Memory Endpoint text field', () => {
      renderComponent();
      expect(
        screen.getByLabelText(/Memory Endpoint/i),
      ).toBeInTheDocument();
    });

    it('renders Load Available Indexes button', () => {
      renderComponent();
      expect(
        screen.getByRole('button', { name: /Load Available Indexes/i }),
      ).toBeInTheDocument();
    });

    it('renders Create New Index button', () => {
      renderComponent();
      expect(
        screen.getByRole('button', { name: /Create New Index/i }),
      ).toBeInTheDocument();
    });

    it('does not render Databricks section when backend type is default', () => {
      storeOverrides = {};
      renderComponent();
      expect(
        screen.queryByText('Databricks Vector Search Configuration'),
      ).not.toBeInTheDocument();
    });
  });

  // -----------------------------------------------------------------------
  // Validate on config change
  // -----------------------------------------------------------------------
  describe('config validation on mount', () => {
    it('calls validateConfig on initial render', async () => {
      renderComponent();
      await waitFor(() => {
        expect(mockValidateConfig).toHaveBeenCalled();
      });
    });

    it('invokes onConfigChange callback with validation result', async () => {
      const onConfigChange = vi.fn();
      mockValidateConfig.mockResolvedValue(true);
      renderComponent({ onConfigChange });
      await waitFor(() => {
        expect(onConfigChange).toHaveBeenCalledWith(true);
      });
    });
  });
});
