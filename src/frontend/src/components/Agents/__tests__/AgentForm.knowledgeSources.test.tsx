import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import AgentForm from '../AgentForm';
import { AgentService } from '../../../api/AgentService';
import { Agent, Tool } from '../../../types/agent';

// Mock AgentService
jest.mock('../../../api/AgentService', () => ({
  AgentService: {
    createAgentFull: jest.fn(),
    updateAgentFull: jest.fn(),
    getAgent: jest.fn(),
  },
}));

describe('AgentForm - Knowledge Sources', () => {
  const mockOnCancel = jest.fn();
  const mockOnAgentSaved = jest.fn();
  
  const mockTools: Tool[] = [
    {
      id: '1',
      name: 'Tool 1',
      description: 'Test tool 1',
      created_at: '2025-01-01',
      updated_at: '2025-01-01',
    },
  ];

  const mockAgentWithKnowledgeSources: Agent = {
    id: 'agent-123',
    name: 'Test Agent',
    role: 'Test Role',
    goal: 'Test Goal',
    backstory: 'Test Backstory',
    tools: [],
    knowledge_sources: [
      {
        type: 'databricks_volume',
        source: '/Volumes/users/test/knowledge/doc1.pdf',
        metadata: {
          filename: 'doc1.pdf',
          execution_id: 'exec-123',
          group_id: 'group-456',
          uploaded_at: '2025-01-01T12:00:00Z',
        },
      },
      {
        type: 'databricks_volume',
        source: '/Volumes/users/test/knowledge/doc2.docx',
        metadata: {
          filename: 'doc2.docx',
          execution_id: 'exec-456',
          group_id: 'group-789',
          uploaded_at: '2025-01-02T14:00:00Z',
        },
      },
    ],
  };

  const defaultProps = {
    tools: mockTools,
    onCancel: mockOnCancel,
    onAgentSaved: mockOnAgentSaved,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('displays knowledge sources section with count', () => {
    render(
      <AgentForm 
        {...defaultProps} 
        agent={mockAgentWithKnowledgeSources}
      />
    );
    
    // Should show knowledge sources count in accordion header
    expect(screen.getByText(/Knowledge Sources \(2\)/i)).toBeInTheDocument();
  });

  it('displays knowledge sources section even when empty', () => {
    const agentWithoutKnowledgeSources = {
      ...mockAgentWithKnowledgeSources,
      knowledge_sources: [],
    };
    
    render(
      <AgentForm 
        {...defaultProps} 
        agent={agentWithoutKnowledgeSources}
      />
    );
    
    // Should show knowledge sources with 0 count
    expect(screen.getByText(/Knowledge Sources \(0\)/i)).toBeInTheDocument();
  });

  it('displays knowledge source details when accordion is expanded', () => {
    render(
      <AgentForm 
        {...defaultProps} 
        agent={mockAgentWithKnowledgeSources}
      />
    );
    
    // Expand the knowledge sources accordion
    const accordion = screen.getByText(/Knowledge Sources \(2\)/i);
    fireEvent.click(accordion);
    
    // Should display file information
    expect(screen.getByText('doc1.pdf')).toBeInTheDocument();
    expect(screen.getByText('doc2.docx')).toBeInTheDocument();
    expect(screen.getByText(/Databricks Volume/i)).toBeInTheDocument();
  });

  it('handles undefined knowledge sources gracefully', () => {
    const agentWithUndefinedKnowledgeSources = {
      ...mockAgentWithKnowledgeSources,
      knowledge_sources: undefined,
    };
    
    render(
      <AgentForm 
        {...defaultProps} 
        agent={agentWithUndefinedKnowledgeSources as Agent}
      />
    );
    
    // Should show 0 count and not crash
    expect(screen.getByText(/Knowledge Sources \(0\)/i)).toBeInTheDocument();
  });

  it('preserves knowledge sources when saving agent', async () => {
    (AgentService.updateAgentFull as jest.Mock).mockResolvedValueOnce({
      ...mockAgentWithKnowledgeSources,
      name: 'Updated Agent',
    });
    
    render(
      <AgentForm 
        {...defaultProps} 
        agent={mockAgentWithKnowledgeSources}
      />
    );
    
    // Change agent name
    const nameInput = screen.getByLabelText(/Agent Name/i);
    fireEvent.change(nameInput, { target: { value: 'Updated Agent' } });
    
    // Save the agent
    const saveButton = screen.getByText(/Save Agent/i);
    fireEvent.click(saveButton);
    
    await waitFor(() => {
      expect(AgentService.updateAgentFull).toHaveBeenCalledWith(
        'agent-123',
        expect.objectContaining({
          name: 'Updated Agent',
          knowledge_sources: mockAgentWithKnowledgeSources.knowledge_sources,
        })
      );
    });
  });

  it('displays appropriate icon for Databricks volume sources', () => {
    render(
      <AgentForm 
        {...defaultProps} 
        agent={mockAgentWithKnowledgeSources}
      />
    );
    
    // Expand the knowledge sources accordion
    const accordion = screen.getByText(/Knowledge Sources \(2\)/i);
    fireEvent.click(accordion);
    
    // Should have cloud icons for Databricks sources
    const cloudIcons = screen.getAllByTestId('CloudUploadIcon');
    expect(cloudIcons).toHaveLength(2);
  });

  it('displays file paths correctly', () => {
    render(
      <AgentForm 
        {...defaultProps} 
        agent={mockAgentWithKnowledgeSources}
      />
    );
    
    // Expand the knowledge sources accordion
    const accordion = screen.getByText(/Knowledge Sources \(2\)/i);
    fireEvent.click(accordion);
    
    // Should display truncated paths
    expect(screen.getByText((content, element) => {
      return element?.textContent?.includes('/Volumes/users/test/knowledge/doc1.pdf') ?? false;
    })).toBeInTheDocument();
  });

  it('handles mixed knowledge source types', () => {
    const agentWithMixedSources = {
      ...mockAgentWithKnowledgeSources,
      knowledge_sources: [
        {
          type: 'databricks_volume',
          source: '/Volumes/users/test/knowledge/doc.pdf',
          metadata: {
            filename: 'doc.pdf',
          },
        },
        {
          type: 'text',
          source: 'Inline text knowledge',
        },
        {
          type: 'url',
          source: 'https://example.com/doc',
        },
      ],
    };
    
    render(
      <AgentForm 
        {...defaultProps} 
        agent={agentWithMixedSources as Agent}
      />
    );
    
    // Should show count of all sources
    expect(screen.getByText(/Knowledge Sources \(3\)/i)).toBeInTheDocument();
    
    // Expand and check different types
    const accordion = screen.getByText(/Knowledge Sources \(3\)/i);
    fireEvent.click(accordion);
    
    expect(screen.getByText('doc.pdf')).toBeInTheDocument();
    expect(screen.getByText(/Text Knowledge/i)).toBeInTheDocument();
    expect(screen.getByText(/URL Knowledge/i)).toBeInTheDocument();
  });

  it('shows no knowledge sources message when empty', () => {
    const agentWithoutSources = {
      ...mockAgentWithKnowledgeSources,
      knowledge_sources: [],
    };
    
    render(
      <AgentForm 
        {...defaultProps} 
        agent={agentWithoutSources}
      />
    );
    
    // Expand the accordion
    const accordion = screen.getByText(/Knowledge Sources \(0\)/i);
    fireEvent.click(accordion);
    
    // Should show empty state message
    expect(screen.getByText(/No knowledge sources configured/i)).toBeInTheDocument();
  });

  it('maintains knowledge sources when creating new agent', async () => {
    const newAgent = {
      name: 'New Agent',
      role: 'New Role',
      goal: 'New Goal',
      backstory: 'New Backstory',
      knowledge_sources: [
        {
          type: 'databricks_volume',
          source: '/Volumes/test/file.pdf',
          metadata: {
            filename: 'file.pdf',
          },
        },
      ],
    };
    
    (AgentService.createAgentFull as jest.Mock).mockResolvedValueOnce({
      id: 'new-agent-id',
      ...newAgent,
    });
    
    render(
      <AgentForm 
        {...defaultProps}
        agent={newAgent as Agent}
        isCreateMode={true}
      />
    );
    
    // Save the new agent
    const saveButton = screen.getByText(/Create Agent/i);
    fireEvent.click(saveButton);
    
    await waitFor(() => {
      expect(AgentService.createAgentFull).toHaveBeenCalledWith(
        expect.objectContaining({
          knowledge_sources: newAgent.knowledge_sources,
        })
      );
    });
  });
});