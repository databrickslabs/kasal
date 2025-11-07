import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { KnowledgeFileUpload } from '../KnowledgeFileUpload';
import { apiClient } from '../../../config/api/ApiConfig';
import { Agent } from '../../../types/agent';
import { AgentService } from '../../../api/AgentService';

// Mock the API client
jest.mock('../../../config/api/ApiConfig', () => ({
  apiClient: {
    post: jest.fn(),
    get: jest.fn(),
  },
}));

// Mock AgentService
jest.mock('../../../api/AgentService', () => ({
  AgentService: {
    updateAgentFull: jest.fn(),
  },
}));

describe('KnowledgeFileUpload', () => {
  const mockOnFilesUploaded = jest.fn();
  const mockOnAgentsUpdated = jest.fn();
  
  const mockAgents: Agent[] = [
    {
      id: 'agent-1',
      name: 'Test Agent 1',
      role: 'Test Role 1',
      goal: 'Test Goal 1',
      backstory: 'Test Backstory 1',
      tools: [],
      knowledge_sources: [],
    },
    {
      id: 'agent-2',
      name: 'Test Agent 2',
      role: 'Test Role 2',
      goal: 'Test Goal 2',
      backstory: 'Test Backstory 2',
      tools: [],
      knowledge_sources: [],
    },
  ];

  const defaultProps = {
    executionId: 'test-execution-123',
    groupId: 'test-group-456',
    onFilesUploaded: mockOnFilesUploaded,
    onAgentsUpdated: mockOnAgentsUpdated,
    availableAgents: mockAgents,
    disabled: false,
    compact: false,
  };

  beforeEach(() => {
    jest.clearAllMocks();
    // Mock the knowledge base config check
    (apiClient.get as jest.Mock).mockResolvedValue({
      data: { configured: true },
    });
  });

  it('renders the upload button', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    await waitFor(() => {
      expect(screen.getByLabelText(/Upload Knowledge Files/i)).toBeInTheDocument();
    });
  });

  it('opens dialog when upload button is clicked', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    await waitFor(() => {
      expect(screen.getByText(/Select Agents for Knowledge Access/i)).toBeInTheDocument();
    });
  });

  it('displays available agents for selection', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    await waitFor(() => {
      expect(screen.getByText('Test Agent 1')).toBeInTheDocument();
      expect(screen.getByText('Test Agent 2')).toBeInTheDocument();
    });
  });

  it('allows agent selection by clicking on chips', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    await waitFor(() => {
      const agent1Chip = screen.getByText('Test Agent 1');
      fireEvent.click(agent1Chip);
    });
    
    // Check if selection info is displayed
    await waitFor(() => {
      expect(screen.getByText(/Selected 1 agent will have access/i)).toBeInTheDocument();
    });
  });

  it('handles file upload from Databricks volume', async () => {
    // Mock successful file selection from volume
    (apiClient.post as jest.Mock).mockResolvedValueOnce({
      data: { path: '/Volumes/test/knowledge/file.pdf' },
    });
    
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    // Open dialog
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    // Switch to Browse Volume tab
    await waitFor(() => {
      const browseTab = screen.getByText('Browse Volume');
      fireEvent.click(browseTab);
    });
    
    // Type file path
    const pathInput = screen.getByLabelText(/File path in Databricks volume/i);
    fireEvent.change(pathInput, { target: { value: '/Volumes/test/knowledge/file.pdf' } });
    
    // Click Select button
    const selectButton = screen.getByText('Select');
    fireEvent.click(selectButton);
    
    await waitFor(() => {
      expect(apiClient.post).toHaveBeenCalledWith(
        `/databricks/knowledge/select-from-volume/test-execution-123`,
        expect.any(FormData)
      );
    });
  });

  it('updates agents with knowledge sources when files are uploaded', async () => {

    
    // Mock successful file upload and agent update
    (apiClient.post as jest.Mock).mockResolvedValueOnce({
      data: { path: '/Volumes/test/knowledge/file.pdf' },
    });
    
    AgentService.updateAgentFull.mockResolvedValueOnce({
      ...mockAgents[0],
      knowledge_sources: [{
        type: 'databricks_volume',
        source: '/Volumes/test/knowledge/file.pdf',
        metadata: {
          filename: 'file.pdf',
          execution_id: 'test-execution-123',
          group_id: 'test-group-456',
        },
      }],
    });
    
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    // Open dialog and select an agent
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    await waitFor(() => {
      const agent1Chip = screen.getByText('Test Agent 1');
      fireEvent.click(agent1Chip);
    });
    
    // Switch to Browse Volume tab
    const browseTab = screen.getByText('Browse Volume');
    fireEvent.click(browseTab);
    
    // Select a file
    const pathInput = screen.getByLabelText(/File path in Databricks volume/i);
    fireEvent.change(pathInput, { target: { value: '/Volumes/test/knowledge/file.pdf' } });
    
    const selectButton = screen.getByText('Select');
    fireEvent.click(selectButton);
    
    await waitFor(() => {
      expect(AgentService.updateAgentFull).toHaveBeenCalledWith(
        'agent-1',
        expect.objectContaining({
          name: 'Test Agent 1',
          role: 'Test Role 1',
        })
      );
      expect(mockOnAgentsUpdated).toHaveBeenCalled();
    });
  });

  it('handles agents without IDs gracefully', async () => {
    const agentsWithoutIds = [
      {
        name: 'New Agent',
        role: 'New Role',
        goal: 'New Goal',
        backstory: 'New Backstory',
        tools: [],
        knowledge_sources: [],
      },
    ];
    
    const propsWithNewAgents = {
      ...defaultProps,
      availableAgents: agentsWithoutIds as Agent[],
    };
    
    render(<KnowledgeFileUpload {...propsWithNewAgents} />);
    
    // Open dialog
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    await waitFor(() => {
      // Agent should be displayed with fallback ID
      expect(screen.getByText('New Agent')).toBeInTheDocument();
    });
    
    // Select the agent
    const agentChip = screen.getByText('New Agent');
    fireEvent.click(agentChip);
    
    // Verify selection works with fallback ID
    await waitFor(() => {
      expect(screen.getByText(/Selected 1 agent will have access/i)).toBeInTheDocument();
    });
  });

  it('disables upload when disabled prop is true', () => {
    render(<KnowledgeFileUpload {...defaultProps} disabled={true} />);
    
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    expect(uploadButton).toBeDisabled();
  });

  it('renders in compact mode', () => {
    render(<KnowledgeFileUpload {...defaultProps} compact={true} />);
    
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    // In compact mode, button should be an icon button
    expect(uploadButton.closest('button')).toHaveClass('MuiIconButton-root');
  });

  it('handles empty agent list', async () => {
    render(<KnowledgeFileUpload {...defaultProps} availableAgents={[]} />);
    
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    await waitFor(() => {
      expect(screen.getByText(/No agents on canvas/i)).toBeInTheDocument();
    });
  });

  it('handles knowledge base not configured', async () => {
    // Mock unconfigured knowledge base
    (apiClient.get as jest.Mock).mockResolvedValueOnce({
      data: { configured: false },
    });
    
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    await waitFor(() => {
      const uploadButton = screen.queryByLabelText(/Upload Knowledge Files/i);
      expect(uploadButton).not.toBeInTheDocument();
    });
  });

  it('handles file upload errors gracefully', async () => {
    const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();
    
    // Mock file upload failure
    (apiClient.post as jest.Mock).mockRejectedValueOnce(
      new Error('Upload failed')
    );
    
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    // Open dialog
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    // Switch to Browse Volume tab
    await waitFor(() => {
      const browseTab = screen.getByText('Browse Volume');
      fireEvent.click(browseTab);
    });
    
    // Try to select a file
    const pathInput = screen.getByLabelText(/File path in Databricks volume/i);
    fireEvent.change(pathInput, { target: { value: '/Volumes/test/file.pdf' } });
    
    const selectButton = screen.getByText('Select');
    fireEvent.click(selectButton);
    
    await waitFor(() => {
      expect(consoleErrorSpy).toHaveBeenCalledWith(
        expect.stringContaining('Failed to select'),
        expect.any(Error)
      );
    });
    
    consoleErrorSpy.mockRestore();
  });

  it('clears selected agents when dialog is closed', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);
    
    // Open dialog
    const uploadButton = screen.getByLabelText(/Upload Knowledge Files/i);
    fireEvent.click(uploadButton);
    
    // Select an agent
    await waitFor(() => {
      const agent1Chip = screen.getByText('Test Agent 1');
      fireEvent.click(agent1Chip);
    });
    
    // Close dialog
    const closeButton = screen.getByLabelText('close');
    fireEvent.click(closeButton);
    
    // Reopen dialog
    fireEvent.click(uploadButton);
    
    await waitFor(() => {
      // No agents should be selected
      expect(screen.queryByText(/Selected 1 agent will have access/i)).not.toBeInTheDocument();
    });
  });
});