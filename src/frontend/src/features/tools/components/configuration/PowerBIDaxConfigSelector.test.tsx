import { vi, beforeEach, describe, test, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { PowerBIDaxConfigSelector } from './PowerBIDaxConfigSelector';

vi.mock('../../../../hooks/usePowerBIOAuth', () => ({
  usePowerBIOAuth: () => ({
    accessToken: null,
    isAuthenticated: false,
    signIn: vi.fn(),
    signOut: vi.fn(),
    userEmail: null,
    isLoading: false,
    error: null,
  }),
}));

const theme = createTheme();
const wrap = (ui: React.ReactNode) => <ThemeProvider theme={theme}>{ui}</ThemeProvider>;

describe('PowerBIDaxConfigSelector — LLM settings', () => {
  const onChange = vi.fn();
  beforeEach(() => vi.clearAllMocks());

  test('asks for no LLM URL or token: the run credentials are used', () => {
    render(wrap(<PowerBIDaxConfigSelector value={{}} onChange={onChange} />));
    expect(screen.queryByLabelText('LLM Workspace URL')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('LLM Token')).not.toBeInTheDocument();
    expect(screen.getByText(/no LLM\s+URL or token is needed/i)).toBeInTheDocument();
  });

  test('dynamic mode adds no LLM URL or token placeholders', () => {
    render(wrap(<PowerBIDaxConfigSelector value={{ mode: 'static' }} onChange={onChange} />));
    fireEvent.click(screen.getByText('Dynamic'));
    const config = onChange.mock.calls[0][0];
    expect(config.mode).toBe('dynamic');
    expect(config).not.toHaveProperty('llm_workspace_url');
    expect(config).not.toHaveProperty('llm_token');
  });
});
