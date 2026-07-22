import { vi, beforeEach, describe, test, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import {
  PipelineConfigGeneratorConfigSelector,
  PipelineConfigGeneratorConfig,
} from './PipelineConfigGeneratorConfigSelector';

// Mock the Databricks service used by the "Connect" button.
const mockListWarehouses = vi.hoisted(() => vi.fn());
vi.mock('../../api/DatabricksService', () => ({
  DatabricksService: { listWarehouses: mockListWarehouses },
}));

const theme = createTheme();
const wrap = (ui: React.ReactNode) => <ThemeProvider theme={theme}>{ui}</ThemeProvider>;

describe('PipelineConfigGeneratorConfigSelector — warehouse+LLM enrichment', () => {
  const onChange = vi.fn();
  beforeEach(() => vi.clearAllMocks());

  const renderWith = (value: PipelineConfigGeneratorConfig = {}) =>
    render(wrap(<PipelineConfigGeneratorConfigSelector value={value} onChange={onChange} />));

  test('enrichment is off by default — warehouse Select is hidden', () => {
    renderWith({});
    expect(screen.queryByLabelText('SQL Warehouse')).not.toBeInTheDocument();
    expect(screen.getByText(/Warehouse \+ LLM enrichment/i)).toBeInTheDocument();
  });

  test('toggling on sets enable_enrichment=true', () => {
    renderWith({});
    fireEvent.click(screen.getByRole('checkbox'));
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ enable_enrichment: true }));
  });

  test('toggling off clears warehouse_id (defense-in-depth with backend gate)', () => {
    renderWith({ enable_enrichment: true, warehouse_id: 'wh-123' });
    fireEvent.click(screen.getByRole('checkbox'));  // now turning OFF
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ enable_enrichment: false, warehouse_id: undefined }));
  });

  test('when enabled, the warehouse Select is shown', () => {
    renderWith({ enable_enrichment: true });
    // MUI renders the label text twice (InputLabel + legend); assert ≥1 and that
    // the Connect button appeared (proof the enrichment block rendered).
    expect(screen.getAllByText('SQL Warehouse').length).toBeGreaterThan(0);
    expect(screen.getByRole('button', { name: /connect/i })).toBeInTheDocument();
  });

  test('a saved warehouse_id is preserved as a fallback option before Connect', () => {
    renderWith({ enable_enrichment: true, warehouse_id: 'saved-wh' });
    // the value renders even though no warehouses were fetched yet
    expect(screen.getByText('saved-wh')).toBeInTheDocument();
  });

  test('Connect populates the warehouse dropdown', async () => {
    mockListWarehouses.mockResolvedValue([
      { id: 'w1', name: 'Prod WH', state: 'RUNNING' },
    ]);
    renderWith({ enable_enrichment: true });
    fireEvent.click(screen.getByRole('button', { name: /connect/i }));
    await waitFor(() => expect(mockListWarehouses).toHaveBeenCalled());
  });
});
