import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, within, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { MemoryBackendSelector } from './MemoryBackendSelector';
import { MemoryBackendType, MemoryBackendConfig as MemoryBackendConfigType } from '../../types/memoryBackend';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Capture props passed to the (mocked) MemoryBackendConfig so we can drive the
// onConfigChange callback that controls the dialog's Save button validity.
const capturedConfigProps: { current: Record<string, unknown> } = { current: {} };

vi.mock('./MemoryBackendConfig', () => ({
  MemoryBackendConfig: (props: Record<string, unknown>) => {
    capturedConfigProps.current = props;
    return (
      <div data-testid="memory-backend-config">
        <button
          data-testid="set-valid"
          onClick={() => (props.onConfigChange as (v: boolean) => void)(true)}
        >
          set-valid
        </button>
        <button
          data-testid="set-invalid"
          onClick={() => (props.onConfigChange as (v: boolean) => void)(false)}
        >
          set-invalid
        </button>
      </div>
    );
  },
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const theme = createTheme();

const renderSelector = (props: Partial<React.ComponentProps<typeof MemoryBackendSelector>> = {}) => {
  const onChange = props.onChange ?? vi.fn();
  const utils = render(
    <ThemeProvider theme={theme}>
      <MemoryBackendSelector onChange={onChange} {...props} />
    </ThemeProvider>
  );
  return { onChange, ...utils };
};

// Opens the MUI Select dropdown and returns the listbox option matching label.
const openSelectAndPick = (label: RegExp | string) => {
  const combobox = screen.getByRole('combobox');
  fireEvent.mouseDown(combobox);
  const listbox = screen.getByRole('listbox');
  const option = within(listbox).getByText(label);
  fireEvent.click(option);
};

const databricksConfigured: MemoryBackendConfigType = {
  backend_type: MemoryBackendType.DATABRICKS,
  databricks_config: {
    endpoint_name: 'ep-1',
    memory_index: 'idx-1',
  },
};

const databricksUnconfigured: MemoryBackendConfigType = {
  backend_type: MemoryBackendType.DATABRICKS,
  databricks_config: {
    endpoint_name: '',
    memory_index: '',
  },
};

const lakebaseInitialized: MemoryBackendConfigType = {
  backend_type: MemoryBackendType.LAKEBASE,
  lakebase_config: {
    embedding_dimension: 2048,
    memory_table: 'crew_memory',
    tables_initialized: true,
  },
};

const lakebaseNotInitialized: MemoryBackendConfigType = {
  backend_type: MemoryBackendType.LAKEBASE,
  lakebase_config: {
    memory_table: 'crew_memory',
    tables_initialized: false,
  },
};

beforeEach(() => {
  vi.clearAllMocks();
  capturedConfigProps.current = {};
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('MemoryBackendSelector', () => {
  it('renders the select with the default backend and a Ready status chip', () => {
    renderSelector();
    // MUI renders the label twice (InputLabel + fieldset legend).
    expect(screen.getAllByText('Memory Backend').length).toBeGreaterThan(0);
    // Default backend shows the "Ready" chip in the selected value.
    expect(screen.getByText('Ready')).toBeInTheDocument();
  });

  it('falls back to DEFAULT_MEMORY_BACKEND_CONFIG when value is undefined', () => {
    // value defaults via the destructuring default; status chip should be Ready.
    render(
      <ThemeProvider theme={theme}>
        <MemoryBackendSelector onChange={vi.fn()} value={undefined} />
      </ThemeProvider>
    );
    expect(screen.getByText('Ready')).toBeInTheDocument();
  });

  it('renders helperText with secondary color when no error', () => {
    renderSelector({ helperText: 'Pick a backend' });
    expect(screen.getByText('Pick a backend')).toBeInTheDocument();
  });

  it('renders helperText with error color when error is true', () => {
    renderSelector({ helperText: 'Required field', error: true });
    expect(screen.getByText('Required field')).toBeInTheDocument();
  });

  it('shows a Configured chip for a fully configured Databricks backend', () => {
    renderSelector({ value: databricksConfigured });
    expect(screen.getByText('Configured')).toBeInTheDocument();
  });

  it('shows a Not Configured chip for an incomplete Databricks backend', () => {
    renderSelector({ value: databricksUnconfigured });
    expect(screen.getByText('Not Configured')).toBeInTheDocument();
  });

  it('renders Databricks quick summary with endpoint and index', () => {
    renderSelector({ value: databricksConfigured });
    expect(screen.getByText(/Endpoint: ep-1 \| Index: idx-1/)).toBeInTheDocument();
  });

  it('shows a Tables Ready chip and lakebase summary for initialized lakebase', () => {
    renderSelector({ value: lakebaseInitialized });
    expect(screen.getByText('Tables Ready')).toBeInTheDocument();
    expect(screen.getByText(/Dimension: 2048 \| Tables: Initialized/)).toBeInTheDocument();
  });

  it('shows a Tables Not Initialized chip and default dimension summary for lakebase', () => {
    renderSelector({ value: lakebaseNotInitialized });
    expect(screen.getByText('Tables Not Initialized')).toBeInTheDocument();
    // embedding_dimension undefined -> default 1024
    expect(screen.getByText(/Dimension: 1024 \| Tables: Not initialized/)).toBeInTheDocument();
  });

  it('does not render a quick summary for the default backend', () => {
    renderSelector();
    expect(screen.queryByText(/Endpoint:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Dimension:/)).not.toBeInTheDocument();
  });

  it('calls onChange with the default config when DEFAULT is selected', () => {
    const { onChange } = renderSelector({ value: lakebaseInitialized });
    openSelectAndPick(/Local \(CrewAI unified Memory \/ LanceDB\)/);
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ backend_type: MemoryBackendType.DEFAULT })
    );
  });

  it('calls onChange and opens dialog when LAKEBASE is selected from default', () => {
    const { onChange } = renderSelector();
    openSelectAndPick(/Lakebase \(pgvector\)/);
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        backend_type: MemoryBackendType.LAKEBASE,
        lakebase_config: expect.objectContaining({
          embedding_dimension: 1024,
          memory_table: 'crew_memory',
          tables_initialized: false,
        }),
      })
    );
    // Dialog opened -> mocked config rendered.
    expect(screen.getByTestId('memory-backend-config')).toBeInTheDocument();
  });

  it('opens the dialog (without onChange) when DATABRICKS is selected from default', () => {
    const { onChange } = renderSelector();
    openSelectAndPick(/Databricks Vector Search/);
    // Databricks branch only opens the dialog, it does NOT call onChange.
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getByTestId('memory-backend-config')).toBeInTheDocument();
  });

  it('opens the dialog from the settings end-adornment icon for non-default backends', () => {
    renderSelector({ value: databricksConfigured });
    const settingsBtn = screen.getByRole('button', { name: /Configure Backend/i });
    fireEvent.click(settingsBtn);
    expect(screen.getByTestId('memory-backend-config')).toBeInTheDocument();
  });

  it('does not render the settings end-adornment for the default backend', () => {
    renderSelector();
    expect(screen.queryByRole('button', { name: /Configure Backend/i })).not.toBeInTheDocument();
  });

  it('closes the dialog via Cancel without calling onChange', () => {
    const { onChange } = renderSelector({ value: databricksConfigured });
    fireEvent.click(screen.getByRole('button', { name: /Configure Backend/i }));
    expect(screen.getByTestId('memory-backend-config')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    // onChange was only used to open; Cancel must not invoke it.
    expect(onChange).not.toHaveBeenCalled();
  });

  it('saves the config when valid and calls onChange with the temp config', async () => {
    const { onChange } = renderSelector({ value: databricksConfigured });
    fireEvent.click(screen.getByRole('button', { name: /Configure Backend/i }));

    // isValid defaults to true, so Save should be enabled and propagate value.
    const saveBtn = screen.getByRole('button', { name: 'Save Configuration' });
    expect(saveBtn).not.toBeDisabled();
    fireEvent.click(saveBtn);

    expect(onChange).toHaveBeenCalledWith(databricksConfigured);
    // The MUI Dialog unmounts its content after the close transition completes.
    await waitFor(() =>
      expect(screen.queryByTestId('memory-backend-config')).not.toBeInTheDocument()
    );
  });

  it('disables Save and does not call onChange when config is invalid', () => {
    const { onChange } = renderSelector({ value: databricksConfigured });
    fireEvent.click(screen.getByRole('button', { name: /Configure Backend/i }));

    // Drive onConfigChange(false) through the mocked child.
    fireEvent.click(screen.getByTestId('set-invalid'));

    const saveBtn = screen.getByRole('button', { name: 'Save Configuration' });
    expect(saveBtn).toBeDisabled();

    // Clicking a disabled button does nothing; also exercise handleSaveConfig guard.
    fireEvent.click(saveBtn);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('re-enables Save after validity is restored', () => {
    const { onChange } = renderSelector({ value: databricksConfigured });
    fireEvent.click(screen.getByRole('button', { name: /Configure Backend/i }));

    fireEvent.click(screen.getByTestId('set-invalid'));
    expect(screen.getByRole('button', { name: 'Save Configuration' })).toBeDisabled();

    fireEvent.click(screen.getByTestId('set-valid'));
    const saveBtn = screen.getByRole('button', { name: 'Save Configuration' });
    expect(saveBtn).not.toBeDisabled();

    fireEvent.click(saveBtn);
    expect(onChange).toHaveBeenCalledWith(databricksConfigured);
  });

  it('passes the embedded prop to the mocked MemoryBackendConfig', () => {
    renderSelector({ value: databricksConfigured });
    fireEvent.click(screen.getByRole('button', { name: /Configure Backend/i }));
    expect(capturedConfigProps.current.embedded).toBe(true);
    expect(typeof capturedConfigProps.current.onConfigChange).toBe('function');
  });

  it('respects the disabled prop on the FormControl', () => {
    renderSelector({ disabled: true });
    // MUI marks the combobox aria-disabled when the control is disabled.
    const combobox = screen.getByRole('combobox');
    expect(combobox).toHaveAttribute('aria-disabled', 'true');
  });

  it('shows lakebase summary without a quick summary line when lakebase_config is absent', () => {
    renderSelector({ value: { backend_type: MemoryBackendType.LAKEBASE } });
    // No lakebase_config -> getQuickSummary returns '' so no summary text is rendered.
    expect(screen.queryByText(/Dimension:/)).not.toBeInTheDocument();
    // Status chip falls into the "not initialized" branch (tables_initialized undefined).
    expect(screen.getByText('Tables Not Initialized')).toBeInTheDocument();
  });
});
