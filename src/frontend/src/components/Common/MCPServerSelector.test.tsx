/**
 * Tests for MCPServerSelector component
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MCPServerSelector } from './MCPServerSelector';
import { MCPService } from '../../api/MCPService';
import { MCPServerConfig } from '../Configuration/MCP/MCPConfiguration';

// Mock MCPService
jest.mock('../../api/MCPService');

const mockMCPService = MCPService as jest.Mocked<typeof MCPService>;

const mockServers: MCPServerConfig[] = [
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
    auth_type: 'databricks_obo',
    timeout_seconds: 45,
    max_retries: 5,
    rate_limit: 100,
  },
  {
    id: 'server3',
    name: 'Disabled Server',
    enabled: false,  // This should be filtered out
    global_enabled: false,
    server_url: 'http://localhost:5002/mcp',
    api_key: 'test-key-3',
    server_type: 'streamable',
    auth_type: 'api_key',
    timeout_seconds: 30,
    max_retries: 3,
    rate_limit: 60,
  },
];

describe('MCPServerSelector', () => {
  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();
    
    // Mock MCPService instance
    const mockInstance = {
      getMcpServers: jest.fn().mockResolvedValue({
        servers: mockServers,
        count: mockServers.length,
      }),
    };
    mockMCPService.getInstance.mockReturnValue(mockInstance as any);
  });

  it('renders with default props', () => {
    const onChange = jest.fn();
    render(<MCPServerSelector value={null} onChange={onChange} />);
    
    expect(screen.getByLabelText('MCP Servers')).toBeInTheDocument();
  });

  it('loads and displays enabled MCP servers when opened', async () => {
    const onChange = jest.fn();
    render(<MCPServerSelector value={null} onChange={onChange} />);
    
    const input = screen.getByLabelText('MCP Servers');
    fireEvent.click(input);
    
    await waitFor(() => {
      expect(screen.getByText('Gmail Server (streamable)')).toBeInTheDocument();
      expect(screen.getByText('Test Server (sse)')).toBeInTheDocument();
      // Disabled server should not be shown
      expect(screen.queryByText('Disabled Server (streamable)')).not.toBeInTheDocument();
    });
  });

  it('handles multiple selection', async () => {
    const onChange = jest.fn();
    render(
      <MCPServerSelector 
        value={[]} 
        onChange={onChange} 
        multiple={true}
      />
    );
    
    const input = screen.getByLabelText('MCP Servers');
    fireEvent.click(input);
    
    await waitFor(() => {
      const gmailOption = screen.getByText('Gmail Server (streamable)');
      fireEvent.click(gmailOption);
    });
    
    expect(onChange).toHaveBeenCalledWith(['server1']);
  });

  it('handles single selection', async () => {
    const onChange = jest.fn();
    render(
      <MCPServerSelector 
        value={null} 
        onChange={onChange} 
        multiple={false}
      />
    );
    
    const input = screen.getByLabelText('MCP Servers');
    fireEvent.click(input);
    
    await waitFor(() => {
      const testOption = screen.getByText('Test Server (sse)');
      fireEvent.click(testOption);
    });
    
    expect(onChange).toHaveBeenCalledWith('server2');
  });

  it('displays error message when servers fail to load', async () => {
    // Mock error response
    const mockInstance = {
      getMcpServers: jest.fn().mockRejectedValue(new Error('Network error')),
    };
    mockMCPService.getInstance.mockReturnValue(mockInstance as any);
    
    const onChange = jest.fn();
    render(<MCPServerSelector value={null} onChange={onChange} />);
    
    const input = screen.getByLabelText('MCP Servers');
    fireEvent.click(input);
    
    await waitFor(() => {
      expect(screen.getByText('Failed to load MCP servers')).toBeInTheDocument();
    });
  });

  it('shows helper text when no servers are available', async () => {
    // Mock empty response
    const mockInstance = {
      getMcpServers: jest.fn().mockResolvedValue({
        servers: [],
        count: 0,
      }),
    };
    mockMCPService.getInstance.mockReturnValue(mockInstance as any);
    
    const onChange = jest.fn();
    render(<MCPServerSelector value={null} onChange={onChange} />);
    
    const input = screen.getByLabelText('MCP Servers');
    fireEvent.click(input);
    
    await waitFor(() => {
      expect(screen.getByText('No enabled MCP servers found')).toBeInTheDocument();
      expect(screen.getByText('Configure MCP servers in Settings → Configuration → MCP')).toBeInTheDocument();
    });
  });

  it('updates selection when value prop changes', async () => {
    const onChange = jest.fn();
    const { rerender } = render(
      <MCPServerSelector value={[]} onChange={onChange} multiple={true} />
    );
    
    // Open to load servers
    const input = screen.getByLabelText('MCP Servers');
    fireEvent.click(input);
    
    await waitFor(() => {
      expect(screen.getByText('Gmail Server (streamable)')).toBeInTheDocument();
    });
    
    // Update value prop
    rerender(
      <MCPServerSelector value={['server1']} onChange={onChange} multiple={true} />
    );
    
    await waitFor(() => {
      expect(screen.getByText('Gmail Server')).toBeInTheDocument(); // Should show as chip
    });
  });
});