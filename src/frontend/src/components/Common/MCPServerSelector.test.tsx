import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { MCPServerSelector } from './MCPServerSelector';

// Create mock instance - must use vi.hoisted
const mockGetMcpServers = vi.hoisted(() => vi.fn());

// Mock MCPService
vi.mock('../../api/MCPService', () => ({
  MCPService: {
    getInstance: vi.fn(() => ({
      getMcpServers: mockGetMcpServers,
    })),
  },
}));

const theme = createTheme();

const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ThemeProvider theme={theme}>
    {children}
  </ThemeProvider>
);

const mockServers = [
  {
    id: 'server1',
    name: 'Gmail Server',
    enabled: true,
    global_enabled: false,
    server_url: 'http://localhost:5000/mcp',
    api_key: 'test-key-1',
    server_type: 'streamable',
    auth_type: 'api_key',
    timeout_seconds: 30,
    max_retries: 3,
    rate_limit: 60,
  },
  {
    id: 'server2',
    name: 'Test Server',
    enabled: true,
    global_enabled: true,
    server_url: 'http://localhost:5001/mcp',
    api_key: 'test-key-2',
    server_type: 'sse',
    auth_type: 'databricks_spn',
    timeout_seconds: 45,
    max_retries: 5,
    rate_limit: 100,
  },
];

describe('MCPServerSelector', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetMcpServers.mockResolvedValue({
      servers: mockServers,
      count: mockServers.length,
    });
  });

  it('renders with default props', () => {
    const onChange = vi.fn();
    render(
      <TestWrapper>
        <MCPServerSelector value={null} onChange={onChange} />
      </TestWrapper>
    );

    expect(screen.getByRole('combobox')).toBeInTheDocument();
  });

  it('shows placeholder text', () => {
    const onChange = vi.fn();
    render(
      <TestWrapper>
        <MCPServerSelector value={null} onChange={onChange} />
      </TestWrapper>
    );

    expect(screen.getByPlaceholderText('Select MCP servers...')).toBeInTheDocument();
  });

  it('loads servers when opened', async () => {
    const onChange = vi.fn();
    render(
      <TestWrapper>
        <MCPServerSelector value={null} onChange={onChange} />
      </TestWrapper>
    );

    const input = screen.getByRole('combobox');
    // Focus and open the dropdown
    fireEvent.focus(input);
    fireEvent.mouseDown(input);

    await waitFor(() => {
      expect(mockGetMcpServers).toHaveBeenCalled();
    }, { timeout: 3000 });
  });

  it('supports custom label', () => {
    const onChange = vi.fn();
    render(
      <TestWrapper>
        <MCPServerSelector value={null} onChange={onChange} label="Custom MCP Label" />
      </TestWrapper>
    );

    // Use getAllByText since MUI renders labels multiple times
    const labels = screen.getAllByText('Custom MCP Label');
    expect(labels.length).toBeGreaterThan(0);
  });

  it('supports disabled state', () => {
    const onChange = vi.fn();
    render(
      <TestWrapper>
        <MCPServerSelector value={null} onChange={onChange} disabled={true} />
      </TestWrapper>
    );

    const input = screen.getByRole('combobox');
    expect(input).toBeDisabled();
  });
});
