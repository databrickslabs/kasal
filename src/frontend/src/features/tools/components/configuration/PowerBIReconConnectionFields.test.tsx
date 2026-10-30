import { vi, beforeEach, describe, test, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { PowerBIReconConnectionFields } from './PowerBIReconConnectionFields';
import type { UCMetricViewGeneratorConfig } from './UCMetricViewGeneratorConfigSelector';

const theme = createTheme();
const wrap = (ui: React.ReactNode) => <ThemeProvider theme={theme}>{ui}</ThemeProvider>;

describe('PowerBIReconConnectionFields', () => {
  const onChange = vi.fn();
  beforeEach(() => vi.clearAllMocks());

  const renderWith = (value: UCMetricViewGeneratorConfig = {}) =>
    render(wrap(<PowerBIReconConnectionFields value={value} onChange={onChange} />));

  test('surfaces workspace + dataset so JSON-mode reconciliation can reach Power BI', () => {
    renderWith({});
    expect(screen.getByLabelText(/Power BI workspace ID/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/dataset \/ semantic-model ID/i)).toBeInTheDocument();
  });

  test('defaults to service principal and shows its credential fields', () => {
    renderWith({});
    expect(screen.getByLabelText(/Client Secret/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/Password/i)).not.toBeInTheDocument();
  });

  test('editing workspace ID writes workspace_id (the key the loop maps to pbi_workspace_id)', () => {
    renderWith({});
    fireEvent.change(screen.getByLabelText(/Power BI workspace ID/i), {
      target: { value: 'ws-1' },
    });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ workspace_id: 'ws-1' }));
  });

  test('switching to Access Token clears SP/SA creds and shows the token field', () => {
    renderWith({
      auth_method: 'service_principal',
      tenant_id: 't',
      client_id: 'c',
      client_secret: 's',
    });
    fireEvent.click(screen.getByRole('button', { name: /Access Token/i }));
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        auth_method: 'user_oauth',
        tenant_id: undefined,
        client_id: undefined,
        client_secret: undefined,
      }),
    );
  });

  test('service account shows username + password', () => {
    renderWith({ auth_method: 'service_account' });
    expect(screen.getByLabelText(/Username/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Password/i)).toBeInTheDocument();
  });
});
