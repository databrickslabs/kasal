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

// Mock LLMProviderService
vi.mock('../../../api/LLMProviderService', () => ({
  LLMProviderService: {
    getInstance: vi.fn(() => ({
      listLLMProviders: vi.fn().mockResolvedValue([]),
    })),
  },
}));

// Mock GenerateService
vi.mock('../../../api/GenerateService', () => ({
  GenerateService: {
    generateTemplates: vi.fn().mockResolvedValue({
      system_template: 'Generated system template',
      prompt_template: 'Generated prompt template',
      response_template: 'Generated response template',
    }),
  },
}));

// Mock stores
vi.mock('../../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: () => ({
    isMemoryBackendConfigured: true,
    isKnowledgeSourceEnabled: true,
  }),
}));

vi.mock('../../../store/agent', () => ({
  useAgentStore: () => ({
    updateAgent: vi.fn(),
  }),
}));

describe('AgentForm - Template Features', () => {
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

  it('renders the form with basic fields', async () => {
    render(<AgentForm {...defaultProps} />);

    await waitFor(() => {
      expect(screen.getByLabelText(/Name/i)).toBeInTheDocument();
    });
  });

  it('renders with template data', async () => {
    const agentWithTemplates = {
      id: 'agent-123',
      name: 'Test Agent',
      role: 'Test Role',
      goal: 'Test Goal',
      backstory: 'Test Backstory',
      system_template: 'System template text',
      prompt_template: 'Prompt template text',
      response_template: 'Response template text',
      tools: [],
      knowledge_sources: [],
    };

    render(<AgentForm {...defaultProps} initialData={agentWithTemplates} />);

    await waitFor(() => {
      const nameInput = screen.getByLabelText(/Name/i) as HTMLInputElement;
      expect(nameInput.value).toBe('Test Agent');
    });
  });
});
