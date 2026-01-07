import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { KnowledgeFileUpload } from '../KnowledgeFileUpload';
import { Agent } from '../../../types/agent';

// Mock the knowledge config store - must use vi.hoisted for variables used in vi.mock
const mockKnowledgeConfigStore = vi.hoisted(() => ({
  isMemoryBackendConfigured: true,
  isKnowledgeSourceEnabled: true,
  isLoading: false,
  refreshConfiguration: vi.fn(),
  checkConfiguration: vi.fn(),
}));

vi.mock('../../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: () => mockKnowledgeConfigStore,
}));

// Mock the agent store
vi.mock('../../../store/agent', () => ({
  useAgentStore: () => ({
    updateAgent: vi.fn(),
  }),
}));

// Mock the API client
vi.mock('../../../config/api/ApiConfig', () => ({
  apiClient: {
    post: vi.fn(),
    get: vi.fn(),
  },
}));

// Mock AgentService
vi.mock('../../../api/AgentService', () => ({
  AgentService: {
    updateAgentFull: vi.fn(),
    getAgent: vi.fn(),
    listAgents: vi.fn().mockResolvedValue([]),
  },
}));

// Mock DatabricksService
vi.mock('../../../api/DatabricksService', () => ({
  DatabricksService: {
    getInstance: vi.fn(() => ({
      getDatabricksConfig: vi.fn().mockResolvedValue({
        knowledge_volume_enabled: true,
        knowledge_volume_path: '/Volumes/test/knowledge',
        workspace_url: 'https://example.com',
      }),
    })),
  },
}));

describe('KnowledgeFileUpload', () => {
  const mockOnFilesUploaded = vi.fn();
  const mockOnAgentsUpdated = vi.fn();

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
    hasAgents: true,
    hasTasks: true,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    // Reset knowledge config to enabled state
    mockKnowledgeConfigStore.isMemoryBackendConfigured = true;
    mockKnowledgeConfigStore.isKnowledgeSourceEnabled = true;
  });

  it('renders the upload button when knowledge is configured', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);

    await waitFor(() => {
      // The button should have the AttachFile icon
      expect(screen.getByTestId('AttachFileIcon')).toBeInTheDocument();
    });
  });

  it('opens dialog when upload button is clicked', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);

    const uploadButton = screen.getByRole('button');
    fireEvent.click(uploadButton);

    await waitFor(() => {
      expect(screen.getByText(/Upload Knowledge Files/i)).toBeInTheDocument();
    });
  });

  it('shows file upload button in dialog when agents are available', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);

    const uploadButton = screen.getByRole('button');
    fireEvent.click(uploadButton);

    await waitFor(() => {
      expect(screen.getByText('Choose Files')).toBeInTheDocument();
    });
  });

  it('disables upload when disabled prop is true', () => {
    render(<KnowledgeFileUpload {...defaultProps} disabled={true} />);

    const uploadButton = screen.getByRole('button');
    expect(uploadButton).toBeDisabled();
  });

  it('renders in compact mode', () => {
    render(<KnowledgeFileUpload {...defaultProps} compact={true} />);

    const uploadButton = screen.getByRole('button');
    // In compact mode, button should be an icon button
    expect(uploadButton).toHaveClass('MuiIconButton-root');
  });

  it('handles empty agent list', async () => {
    render(<KnowledgeFileUpload {...defaultProps} availableAgents={[]} />);

    const uploadButton = screen.getByRole('button');
    fireEvent.click(uploadButton);

    await waitFor(() => {
      expect(screen.getByText(/No agents available on the canvas/i)).toBeInTheDocument();
    });
  });

  it('disables button when knowledge base not configured', async () => {
    // Set memory backend to not configured
    mockKnowledgeConfigStore.isMemoryBackendConfigured = false;
    mockKnowledgeConfigStore.isKnowledgeSourceEnabled = false;

    render(<KnowledgeFileUpload {...defaultProps} />);

    const uploadButton = screen.getByRole('button');
    expect(uploadButton).toBeDisabled();
  });
});
