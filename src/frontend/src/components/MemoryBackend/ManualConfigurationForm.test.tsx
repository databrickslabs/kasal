import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { ManualConfigurationForm } from './ManualConfigurationForm';
import { EMBEDDING_MODELS } from './constants';
import { ManualConfig } from '../../types/memoryBackend';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const makeConfig = (overrides: Partial<ManualConfig> = {}): ManualConfig => ({
  workspace_url: '',
  endpoint_name: '',
  document_endpoint_name: '',
  memory_index: '',
  document_index: '',
  embedding_model: EMBEDDING_MODELS[0].value,
  ...overrides,
});

// A fully-valid configuration (all required fields present, valid index names).
const validConfig = (): ManualConfig =>
  makeConfig({
    workspace_url: 'https://workspace.databricks.com',
    endpoint_name: 'kasal_memory_endpoint',
    document_endpoint_name: 'kasal_docs_endpoint',
    memory_index: 'ml.agents.crew_memory',
    document_index: 'ml.agents.document_embeddings',
  });

const renderForm = (props: Partial<React.ComponentProps<typeof ManualConfigurationForm>> = {}) => {
  const onConfigChange = props.onConfigChange ?? vi.fn();
  const onSave = props.onSave ?? vi.fn();
  const result = render(
    <ThemeProvider theme={createTheme()}>
      <ManualConfigurationForm
        manualConfig={props.manualConfig ?? makeConfig()}
        isSettingUp={props.isSettingUp ?? false}
        error={props.error ?? ''}
        onConfigChange={onConfigChange}
        onSave={onSave}
      />
    </ThemeProvider>,
  );
  return { ...result, onConfigChange, onSave };
};

const getField = (label: string): HTMLInputElement =>
  screen.getByRole('textbox', { name: new RegExp(label, 'i') }) as HTMLInputElement;

describe('ManualConfigurationForm', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the informational alert and all input fields', () => {
    renderForm();
    expect(
      screen.getByText(/Enter your existing Databricks Vector Search configuration/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/Format for indexes:/i)).toBeInTheDocument();
    expect(getField('Databricks Workspace URL')).toBeInTheDocument();
    expect(getField('Memory Endpoint Name')).toBeInTheDocument();
    expect(getField('Document Endpoint Name')).toBeInTheDocument();
    expect(getField('Unified Memory Index')).toBeInTheDocument();
    expect(getField('Document Embeddings Index')).toBeInTheDocument();
    expect(screen.getByText('Vector Search Endpoints')).toBeInTheDocument();
  });

  it('does not render the error alert when error is empty', () => {
    renderForm({ error: '' });
    expect(screen.queryByText('Something went wrong')).not.toBeInTheDocument();
  });

  it('renders the error alert when error is provided', () => {
    renderForm({ error: 'Something went wrong' });
    expect(screen.getByText('Something went wrong')).toBeInTheDocument();
  });

  it('reflects existing config values in the input fields', () => {
    const config = validConfig();
    renderForm({ manualConfig: config });
    expect(getField('Databricks Workspace URL')).toHaveValue(config.workspace_url);
    expect(getField('Memory Endpoint Name')).toHaveValue(config.endpoint_name);
    expect(getField('Document Endpoint Name')).toHaveValue(config.document_endpoint_name);
    expect(getField('Unified Memory Index')).toHaveValue(config.memory_index);
    expect(getField('Document Embeddings Index')).toHaveValue(config.document_index);
  });

  it('calls onConfigChange when the workspace URL changes', () => {
    const config = makeConfig();
    const { onConfigChange } = renderForm({ manualConfig: config });
    fireEvent.change(getField('Databricks Workspace URL'), {
      target: { value: 'https://new.databricks.com' },
    });
    expect(onConfigChange).toHaveBeenCalledWith({
      ...config,
      workspace_url: 'https://new.databricks.com',
    });
  });

  it('calls onConfigChange when the memory endpoint name changes', () => {
    const config = makeConfig();
    const { onConfigChange } = renderForm({ manualConfig: config });
    fireEvent.change(getField('Memory Endpoint Name'), {
      target: { value: 'my_endpoint' },
    });
    expect(onConfigChange).toHaveBeenCalledWith({ ...config, endpoint_name: 'my_endpoint' });
  });

  it('calls onConfigChange when the document endpoint name changes', () => {
    const config = makeConfig();
    const { onConfigChange } = renderForm({ manualConfig: config });
    fireEvent.change(getField('Document Endpoint Name'), {
      target: { value: 'docs_endpoint' },
    });
    expect(onConfigChange).toHaveBeenCalledWith({
      ...config,
      document_endpoint_name: 'docs_endpoint',
    });
  });

  it('calls onConfigChange when the memory index changes', () => {
    const config = makeConfig();
    const { onConfigChange } = renderForm({ manualConfig: config });
    fireEvent.change(getField('Unified Memory Index'), {
      target: { value: 'ml.agents.memory' },
    });
    expect(onConfigChange).toHaveBeenCalledWith({ ...config, memory_index: 'ml.agents.memory' });
  });

  it('calls onConfigChange when the document index changes', () => {
    const config = makeConfig();
    const { onConfigChange } = renderForm({ manualConfig: config });
    fireEvent.change(getField('Document Embeddings Index'), {
      target: { value: 'ml.agents.docs' },
    });
    expect(onConfigChange).toHaveBeenCalledWith({ ...config, document_index: 'ml.agents.docs' });
  });

  it('renders all embedding model options and reflects the selected value', () => {
    const config = makeConfig({ embedding_model: EMBEDDING_MODELS[1].value });
    renderForm({ manualConfig: config });
    // Open the select dropdown.
    const select = screen.getByRole('combobox');
    fireEvent.mouseDown(select);
    const listbox = within(screen.getByRole('listbox'));
    EMBEDDING_MODELS.forEach((model) => {
      expect(
        listbox.getByText(`${model.name} (${model.dimension} dimensions)`),
      ).toBeInTheDocument();
    });
  });

  it('calls onConfigChange when an embedding model is selected', () => {
    const config = makeConfig({ embedding_model: EMBEDDING_MODELS[0].value });
    const { onConfigChange } = renderForm({ manualConfig: config });
    const select = screen.getByRole('combobox');
    fireEvent.mouseDown(select);
    const listbox = within(screen.getByRole('listbox'));
    const target = EMBEDDING_MODELS[2];
    fireEvent.click(listbox.getByText(`${target.name} (${target.dimension} dimensions)`));
    expect(onConfigChange).toHaveBeenCalledWith({ ...config, embedding_model: target.value });
  });

  it('shows the default helper text and no error for a valid memory index', () => {
    renderForm({ manualConfig: validConfig() });
    expect(
      screen.getByText(/Must use UNIFIED_SCHEMA \(one index replaces short-term\/long-term\/entity\)\./i),
    ).toBeInTheDocument();
    expect(
      screen.queryByText('Invalid format. Use: catalog.schema.index_name'),
    ).not.toBeInTheDocument();
  });

  it('shows the validation error helper text for an invalid memory index', () => {
    renderForm({ manualConfig: makeConfig({ memory_index: 'invalid_name' }) });
    expect(
      screen.getByText('Invalid format. Use: catalog.schema.index_name'),
    ).toBeInTheDocument();
  });

  it('treats an empty memory index as not-invalid (no error helper)', () => {
    // Empty string should not trigger the invalid-format helper text.
    renderForm({ manualConfig: makeConfig({ memory_index: '' }) });
    expect(
      screen.queryByText('Invalid format. Use: catalog.schema.index_name'),
    ).not.toBeInTheDocument();
  });

  it('shows the validation error helper text for an invalid document index', () => {
    renderForm({ manualConfig: makeConfig({ document_index: 'bad' }) });
    expect(
      screen.getByText('Invalid format. Use: catalog.schema.index_name'),
    ).toBeInTheDocument();
  });

  it('shows an empty helper text for a valid document index (no error)', () => {
    renderForm({ manualConfig: validConfig() });
    // Document index has empty helperText when valid; only the memory index
    // helper text is the descriptive one, never the invalid message.
    expect(
      screen.queryByText('Invalid format. Use: catalog.schema.index_name'),
    ).not.toBeInTheDocument();
  });

  it('renders the Save button enabled with a fully valid config', () => {
    renderForm({ manualConfig: validConfig() });
    const button = screen.getByRole('button', { name: /Save Configuration/i });
    expect(button).toBeEnabled();
  });

  it('calls onSave when the Save button is clicked with a valid config', () => {
    const { onSave } = renderForm({ manualConfig: validConfig() });
    fireEvent.click(screen.getByRole('button', { name: /Save Configuration/i }));
    expect(onSave).toHaveBeenCalledTimes(1);
  });

  it('disables the Save button when workspace_url is missing', () => {
    renderForm({ manualConfig: { ...validConfig(), workspace_url: '' } });
    expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled();
  });

  it('disables the Save button when endpoint_name is missing', () => {
    renderForm({ manualConfig: { ...validConfig(), endpoint_name: '' } });
    expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled();
  });

  it('disables the Save button when document_endpoint_name is missing', () => {
    renderForm({ manualConfig: { ...validConfig(), document_endpoint_name: '' } });
    expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled();
  });

  it('disables the Save button when memory_index is missing', () => {
    renderForm({ manualConfig: { ...validConfig(), memory_index: '' } });
    expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled();
  });

  it('disables the Save button when document_index is missing', () => {
    renderForm({ manualConfig: { ...validConfig(), document_index: '' } });
    expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled();
  });

  it('disables the Save button when memory_index is invalid', () => {
    renderForm({ manualConfig: { ...validConfig(), memory_index: 'invalid' } });
    expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled();
  });

  it('disables the Save button when document_index is invalid', () => {
    renderForm({ manualConfig: { ...validConfig(), document_index: 'invalid' } });
    expect(screen.getByRole('button', { name: /Save Configuration/i })).toBeDisabled();
  });

  it('shows the loading state, disables inputs and the button while setting up', () => {
    renderForm({ manualConfig: validConfig(), isSettingUp: true });
    expect(screen.getByText('Saving Configuration...')).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /Saving Configuration/i }),
    ).toBeDisabled();
    expect(getField('Databricks Workspace URL')).toBeDisabled();
    expect(getField('Memory Endpoint Name')).toBeDisabled();
    expect(getField('Document Endpoint Name')).toBeDisabled();
    expect(getField('Unified Memory Index')).toBeDisabled();
    expect(getField('Document Embeddings Index')).toBeDisabled();
  });

  it('shows the idle button label when not setting up', () => {
    renderForm({ manualConfig: validConfig(), isSettingUp: false });
    expect(screen.getByText('Save Configuration')).toBeInTheDocument();
    expect(screen.queryByText('Saving Configuration...')).not.toBeInTheDocument();
  });
});
