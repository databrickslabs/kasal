import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import AgentForm from '../AgentForm';

// Mock AgentService
vi.mock('../../../api/AgentService', () => ({
  AgentService: {
    createAgentFull: vi.fn(),
    updateAgentFull: vi.fn(),
    getAgent: vi.fn(),
  },
}));

// Mock ToolService
vi.mock('../../../api/ToolService', () => ({
  ToolService: {
    listTools: vi.fn().mockResolvedValue([]),
  },
}));

// Mock LLMProviderService
vi.mock('../../../api/LLMProviderService', () => ({
  LLMProviderService: {
    getInstance: vi.fn(() => ({
      listLLMProviders: vi.fn().mockResolvedValue([]),
    })),
  },
}));

// Mock ModelService
vi.mock('../../../api/ModelService', () => ({
  ModelService: {
    listModels: vi.fn().mockResolvedValue({
      'test-model': {
        name: 'test-model',
        temperature: 0.7,
        context_window: 4096,
        max_output_tokens: 1024,
        enabled: true,
      },
    }),
  },
}));

// Mock stores
vi.mock('../../../store/agent', () => ({
  useAgentStore: () => ({
    updateAgent: vi.fn(),
  }),
}));
vi.mock('../../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: () => ({
    isMemoryBackendConfigured: true,
    isKnowledgeSourceEnabled: true,
  }),
}));

describe('AgentForm - Basic Rendering', () => {
  const mockOnCancel = vi.fn();
  const mockOnAgentSaved = vi.fn();

  const defaultProps = {
    tools: [],
    onCancel: mockOnCancel,
    onAgentSaved: mockOnAgentSaved,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the form with required fields', async () => {
    render(<AgentForm {...defaultProps} />);

    await waitFor(() => {
      // Check that key form fields are rendered
      expect(screen.getByLabelText(/Name/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Role/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Goal/i)).toBeInTheDocument();
    });
  });

  it('renders with an existing agent', async () => {
    const existingAgent = {
      id: 'agent-123',
      name: 'Test Agent',
      role: 'Test Role',
      goal: 'Test Goal',
      backstory: 'Test Backstory',
      tools: [],
      knowledge_sources: [],
    };

    render(<AgentForm {...defaultProps} initialData={existingAgent} />);

    await waitFor(() => {
      const nameInput = screen.getByLabelText(/Name/i) as HTMLInputElement;
      expect(nameInput.value).toBe('Test Agent');
    });
  });

  it('calls onCancel when cancel button is clicked', async () => {
    render(<AgentForm {...defaultProps} />);

    await waitFor(() => {
      const cancelButton = screen.getByRole('button', { name: /Cancel/i });
      cancelButton.click();
    });

    expect(mockOnCancel).toHaveBeenCalled();
  });
});
