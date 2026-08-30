/**
 * Choosing a model changes which parameters the form offers.
 *
 * A control for a parameter the endpoint refuses does not degrade — the run fails
 * with a 400, because there is no drop_params net on this path. `temperature` is
 * the case that bit: it is seeded for every model in the catalogue, and
 * claude-opus-5 answers it with "Model global.anthropic.claude-opus-5 does not
 * support the temperature parameter".
 *
 * Refusals do NOT follow model families and cannot be inferred here, which is why
 * `refused_params` is derived server-side from measured per-model capability
 * (backend core/llm/model_capabilities.py) and merely consumed by this form. The
 * fixtures below carry the real measured values.
 */
import { vi, beforeEach, describe, it, expect, Mock } from 'vitest';
import React from 'react';
import { render, screen, act, waitFor, fireEvent } from '@testing-library/react';
import AgentForm from './AgentForm';
import { AgentService } from '../../api/workflow/AgentService';

vi.mock('../../api/workflow/AgentService', () => ({
  AgentService: {
    createAgent: vi.fn(),
    updateAgentFull: vi.fn(),
    getAgent: vi.fn(),
  },
}));

vi.mock('../../api/tools/ToolService', () => ({
  ToolService: { listTools: vi.fn().mockResolvedValue([]) },
}));

// The measured capability of three real models. `refused_params`,
// `thinking_mode` and `allowed_efforts` all arrive from the API.
vi.mock('../../api/config/ModelService', () => ({
  ModelService: {
    getInstance: vi.fn(() => ({
      getActiveModels: vi.fn().mockResolvedValue({
        // Accepts temperature; takes a thinking BUDGET.
        'databricks-claude-sonnet-4-5': {
          name: 'databricks-claude-sonnet-4-5',
          provider: 'databricks',
          enabled: true,
          thinking_mode: 'manual',
          allowed_efforts: [],
          refused_params: ['frequency_penalty', 'presence_penalty'],
          returns_thinking_text: true,
        },
        // REFUSES temperature; takes an EFFORT with five levels.
        'databricks-claude-opus-5': {
          name: 'databricks-claude-opus-5',
          provider: 'databricks',
          enabled: true,
          thinking_mode: 'adaptive',
          allowed_efforts: ['low', 'medium', 'high', 'xhigh', 'max'],
          refused_params: [
            'temperature',
            'top_p',
            'frequency_penalty',
            'presence_penalty',
          ],
          returns_thinking_text: true,
        },
        // Accepts temperature; no thinking surface at all.
        'databricks-llama-4-maverick': {
          name: 'databricks-llama-4-maverick',
          provider: 'databricks',
          enabled: true,
          max_output_tokens: 8192,
          thinking_mode: null,
          allowed_efforts: [],
          refused_params: [],
          returns_thinking_text: false,
        },
      }),
    })),
  },
}));

vi.mock('../../api/LLMProviderService', () => ({
  LLMProviderService: {
    getInstance: vi.fn(() => ({ listLLMProviders: vi.fn().mockResolvedValue([]) })),
  },
}));

vi.mock('../../api/workflow/GenerateService', () => ({
  GenerateService: { generateTemplates: vi.fn() },
}));

vi.mock('../../api/memory/DefaultMemoryBackendService', () => ({
  DefaultMemoryBackendService: {
    getInstance: vi.fn(() => ({ getDefaultConfig: vi.fn().mockReturnValue(null) })),
  },
}));

vi.mock('../../api/databricks/DatabricksService', () => ({
  DatabricksService: {
    getInstance: vi.fn(() => ({ getDatabricksConfig: vi.fn().mockResolvedValue(null) })),
  },
}));

vi.mock('../../store/agent', () => ({
  useAgentStore: () => ({ updateAgent: vi.fn() }),
}));

vi.mock('../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: () => ({
    isMemoryBackendConfigured: true,
    isKnowledgeSourceEnabled: true,
  }),
}));

vi.mock('../Common/GenieSpaceSelector', () => ({
  GenieSpaceSelector: () => <div />,
}));
vi.mock('../Common/PerplexityConfigSelector', () => ({
  PerplexityConfigSelector: () => <div />,
}));
vi.mock('../Common/SerperConfigSelector', () => ({
  SerperConfigSelector: () => <div />,
}));
vi.mock('../Common/MCPServerSelector', () => ({
  MCPServerSelector: () => <div />,
}));
vi.mock('../BestPractices/AgentBestPractices', () => ({
  default: () => <div />,
}));

describe('AgentForm — controls follow the selected model', () => {
  const props = {
    tools: [],
    onCancel: vi.fn(),
    onAgentSaved: vi.fn(),
  };

  const renderWithModel = async (llm: string) => {
    render(
      <AgentForm
        {...props}
        initialData={{
          id: 'a1',
          name: 'A',
          role: 'R',
          goal: 'G',
          backstory: 'B',
          llm,
          tools: [],
        } as never}
      />,
    );
    // Models load asynchronously; the gating cannot resolve until they arrive.
    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 150));
    });
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('hides Temperature Override for a model that refuses temperature', async () => {
    // THE case: claude-opus-5 returns a 400 for `temperature`, so an override
    // field here can only ever break a run.
    await renderWithModel('databricks-claude-opus-5');
    await waitFor(() => {
      expect(screen.queryByLabelText(/Temperature Override/i)).not.toBeInTheDocument();
    });
  });

  it('shows Temperature Override for a model that accepts it', async () => {
    // Same provider, same generation prefix, opposite answer — which is why this
    // is per-model data and not a family rule.
    await renderWithModel('databricks-claude-sonnet-4-5');
    await waitFor(() => {
      expect(screen.getByLabelText(/Temperature Override/i)).toBeInTheDocument();
    });
  });

  it('shows Temperature Override for a model with no declared refusals', async () => {
    // Unset stays unset: a model that declares nothing behaves exactly as it did
    // before capability gating existed.
    await renderWithModel('databricks-llama-4-maverick');
    await waitFor(() => {
      expect(screen.getByLabelText(/Temperature Override/i)).toBeInTheDocument();
    });
  });

  it('offers a thinking BUDGET override on a manual model', async () => {
    await renderWithModel('databricks-claude-sonnet-4-5');
    await waitFor(() => {
      expect(screen.getByLabelText(/Thinking Budget Override/i)).toBeInTheDocument();
    });
  });

  it('offers an EFFORT override, not a budget, on an adaptive model', async () => {
    // The two shapes are mutually exclusive: opus-5 rejects a budget outright.
    await renderWithModel('databricks-claude-opus-5');
    await waitFor(() => {
      expect(screen.queryByLabelText(/Thinking Budget Override/i)).not.toBeInTheDocument();
    });
    expect(screen.getAllByText(/Reasoning Effort Override/i).length).toBeGreaterThan(0);
  });

  it('offers no thinking control at all when the model has no surface', async () => {
    await renderWithModel('databricks-llama-4-maverick');
    await waitFor(() => {
      expect(screen.queryByLabelText(/Thinking Budget Override/i)).not.toBeInTheDocument();
    });
    expect(screen.queryByText(/Reasoning Effort Override/i)).not.toBeInTheDocument();
  });

  it('no longer offers Function Calling LLM', async () => {
    // Removed: runtime/agent.py declares the field "Deprecated; accepted for
    // compatibility and unused", no execution path reads it, and its only reader
    // is not invoked by any of the three paths. A control that changes nothing is
    // worse than no control.
    await renderWithModel('databricks-claude-sonnet-4-5');
    await waitFor(() => {
      expect(screen.queryByText(/Function Calling LLM/i)).not.toBeInTheDocument();
    });
  });
});

describe('AgentForm — max output tokens override', () => {
  const props = { tools: [], onCancel: vi.fn(), onAgentSaved: vi.fn() };

  const renderEditing = async (extra: Record<string, unknown> = {}) => {
    render(
      <AgentForm
        {...props}
        initialData={{
          id: 'a1',
          name: 'A',
          role: 'R',
          goal: 'G',
          backstory: 'B',
          llm: 'databricks-llama-4-maverick',
          tools: [],
          ...extra,
        } as never}
      />,
    );
    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 150));
    });
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('offers the override and names the model ceiling that blank inherits', async () => {
    await renderEditing();
    expect(screen.getByLabelText(/Max Output Tokens Override/i)).toBeInTheDocument();
    expect(screen.getByText(/max_output_tokens \(8192\)/)).toBeInTheDocument();
  });

  it('saves the typed value, and null when cleared so the row is actually cleared', async () => {
    (AgentService.updateAgentFull as Mock).mockResolvedValue({ id: 'a1' });
    await renderEditing({ max_tokens: 16000 });
    const field = screen.getByLabelText(/Max Output Tokens Override/i) as HTMLInputElement;
    expect(field.value).toBe('16000');

    fireEvent.change(field, { target: { value: '32000' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(AgentService.updateAgentFull).toHaveBeenCalled());
    expect((AgentService.updateAgentFull as Mock).mock.calls[0][1].max_tokens).toBe(32000);

    fireEvent.change(field, { target: { value: '' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(AgentService.updateAgentFull).toHaveBeenCalledTimes(2));
    // JSON drops undefined; null is what the API clears on.
    expect((AgentService.updateAgentFull as Mock).mock.calls[1][1].max_tokens).toBeNull();
  });
});

