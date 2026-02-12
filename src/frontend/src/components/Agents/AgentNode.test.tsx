/**
 * Unit tests for AgentNode component.
 *
 * Tests the LLM selection flow on the agent node including:
 * - Persisting model changes to the backend via AgentService.updateAgentFull
 * - Skipping backend calls when agentId is missing
 * - Handling backend errors gracefully while keeping local state updated
 * - Updating local node data and agent store on model change
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material';
import AgentNode from './AgentNode';

// Mock ReactFlow - setNodes invokes the callback to achieve coverage of inner logic
const mockSetNodes = vi.fn((updater: unknown) => {
  if (typeof updater === 'function') {
    updater([
      { id: 'agent-node-1', data: { agentId: 'agent-123', label: 'Test Agent', llm: 'databricks-llama-4-maverick' } },
      { id: 'other-node', data: { agentId: 'other', label: 'Other' } },
    ]);
  }
});
const mockSetEdges = vi.fn();
const mockGetNodes = vi.fn(() => []);
const mockGetEdges = vi.fn(() => []);
vi.mock('reactflow', () => ({
  Handle: ({ position, type, id: handleId, style }: { position: string; type: string; id: string; style?: object }) => (
    <div data-testid={`handle-${type}-${handleId || position}`} data-position={position} style={style} />
  ),
  Position: {
    Top: 'top',
    Bottom: 'bottom',
    Left: 'left',
    Right: 'right',
  },
  useReactFlow: () => ({
    setNodes: mockSetNodes,
    setEdges: mockSetEdges,
    getNodes: mockGetNodes,
    getEdges: mockGetEdges,
  }),
}));

// Mock stores
const mockUpdateAgent = vi.fn();
const mockGetAgent = vi.fn(() => Promise.resolve(null));
vi.mock('../../store/agent', () => ({
  useAgentStore: () => ({
    getAgent: mockGetAgent,
    updateAgent: mockUpdateAgent,
  }),
}));

vi.mock('../../store/uiLayout', () => ({
  useUILayoutStore: vi.fn((selector) => {
    const state = { layoutOrientation: 'vertical' };
    return selector(state);
  }),
}));

vi.mock('../../store/crewExecution', () => ({
  useCrewExecutionStore: vi.fn((selector) => {
    const state = { processType: 'sequential' };
    return selector(state);
  }),
}));

// Mock hooks
const mockMarkCurrentTabDirty = vi.fn();
vi.mock('../../hooks/workflow/useTabDirtyState', () => ({
  useTabDirtyState: () => ({
    markCurrentTabDirty: mockMarkCurrentTabDirty,
  }),
}));

// Mock ToolService
vi.mock('../../api/ToolService', () => ({
  ToolService: {
    listEnabledTools: vi.fn(() => Promise.resolve([])),
  },
  Tool: {},
}));

// Mock AgentService
const mockUpdateAgentFull = vi.fn(() => Promise.resolve({ id: 'agent-123', name: 'Test Agent', llm: 'gpt-4' }));
vi.mock('../../api/AgentService', () => ({
  Agent: {},
  AgentService: {
    updateAgentFull: (...args: unknown[]) => mockUpdateAgentFull(...args),
  },
}));

// Mock child components
vi.mock('./AgentForm', () => ({
  default: () => <div data-testid="agent-form">Agent Form</div>,
}));

vi.mock('./LLMSelectionDialog', () => ({
  default: ({ open, onSelectLLM }: { open: boolean; onSelectLLM: (model: string) => void }) =>
    open ? (
      <div data-testid="llm-dialog">
        <button data-testid="select-llm-btn" onClick={() => onSelectLLM('gpt-4o')}>
          Select LLM
        </button>
      </div>
    ) : null,
}));

const theme = createTheme();

const mockAgent = {
  id: 'agent-123',
  name: 'Test Agent',
  role: 'Researcher',
  goal: 'Research things',
  backstory: 'An expert researcher',
  llm: 'databricks-llama-4-maverick',
  tools: [],
  max_iter: 25,
  verbose: false,
  allow_delegation: false,
  cache: true,
  allow_code_execution: false,
  code_execution_mode: 'safe' as const,
};

const defaultProps = {
  id: 'agent-node-1',
  data: {
    agentId: 'agent-123',
    label: 'Test Agent',
    role: 'Researcher',
    goal: 'Research things',
    backstory: 'An expert researcher',
    llm: 'databricks-llama-4-maverick',
    tools: [],
  },
};

const renderAgentNode = (props = {}) => {
  const mergedProps = { ...defaultProps, ...props };
  return render(
    <ThemeProvider theme={theme}>
      <AgentNode {...mergedProps} />
    </ThemeProvider>
  );
};

describe('AgentNode - LLM Selection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetAgent.mockResolvedValue(mockAgent);
  });

  it('should open LLM dialog when clicking on the LLM badge', async () => {
    renderAgentNode();

    // Wait for agent data to load
    await waitFor(() => {
      expect(mockGetAgent).toHaveBeenCalledWith('agent-123');
    });

    // Find and click the LLM badge text
    const llmBadge = screen.getByText('databricks-llama-4-maverick');
    fireEvent.click(llmBadge);

    await waitFor(() => {
      expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
    });
  });

  it('should persist LLM selection to backend when agent has an agentId', async () => {
    mockUpdateAgentFull.mockResolvedValueOnce({ ...mockAgent, llm: 'gpt-4o' });
    renderAgentNode();

    // Wait for agent data to load
    await waitFor(() => {
      expect(mockGetAgent).toHaveBeenCalledWith('agent-123');
    });

    // Open LLM dialog
    const llmBadge = screen.getByText('databricks-llama-4-maverick');
    fireEvent.click(llmBadge);
    await waitFor(() => {
      expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
    });

    // Select an LLM via the mock dialog button
    fireEvent.click(screen.getByTestId('select-llm-btn'));

    // Verify backend call was made with updated agent
    await waitFor(() => {
      expect(mockUpdateAgentFull).toHaveBeenCalledWith(
        'agent-123',
        expect.objectContaining({ llm: 'gpt-4o' })
      );
    });
  });

  it('should update local node state when LLM is selected', async () => {
    renderAgentNode();

    await waitFor(() => {
      expect(mockGetAgent).toHaveBeenCalledWith('agent-123');
    });

    // Open LLM dialog
    const llmBadge = screen.getByText('databricks-llama-4-maverick');
    fireEvent.click(llmBadge);
    await waitFor(() => {
      expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
    });

    // Select an LLM
    fireEvent.click(screen.getByTestId('select-llm-btn'));

    // Verify local state updates
    expect(mockSetNodes).toHaveBeenCalled();
    expect(mockUpdateAgent).toHaveBeenCalledWith(
      'agent-123',
      expect.objectContaining({ llm: 'gpt-4o' })
    );
  });

  it('should mark tab as dirty when LLM is changed', async () => {
    renderAgentNode();

    await waitFor(() => {
      expect(mockGetAgent).toHaveBeenCalledWith('agent-123');
    });

    // Open LLM dialog and select
    const llmBadge = screen.getByText('databricks-llama-4-maverick');
    fireEvent.click(llmBadge);
    await waitFor(() => {
      expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId('select-llm-btn'));

    await waitFor(() => {
      expect(mockMarkCurrentTabDirty).toHaveBeenCalled();
    });
  });

  it('should not call backend when agent has no agentId', async () => {
    mockGetAgent.mockResolvedValue(null);
    renderAgentNode({
      data: {
        ...defaultProps.data,
        agentId: '',
      },
    });

    // The LLM badge should still show the default model
    const llmBadge = screen.getByText('databricks-llama-4-maverick');
    fireEvent.click(llmBadge);

    await waitFor(() => {
      expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId('select-llm-btn'));

    // Local node state should still be updated
    expect(mockSetNodes).toHaveBeenCalled();
    // But backend should NOT be called since agentId is empty
    expect(mockUpdateAgentFull).not.toHaveBeenCalled();
  });

  it('should update local state even if backend call fails', async () => {
    const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    mockUpdateAgentFull.mockRejectedValueOnce(new Error('Network error'));
    renderAgentNode();

    await waitFor(() => {
      expect(mockGetAgent).toHaveBeenCalledWith('agent-123');
    });

    // Open LLM dialog and select
    const llmBadge = screen.getByText('databricks-llama-4-maverick');
    fireEvent.click(llmBadge);
    await waitFor(() => {
      expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId('select-llm-btn'));

    // Local state should still be updated
    expect(mockSetNodes).toHaveBeenCalled();
    expect(mockUpdateAgent).toHaveBeenCalledWith(
      'agent-123',
      expect.objectContaining({ llm: 'gpt-4o' })
    );

    // Backend was called but failed
    await waitFor(() => {
      expect(consoleErrorSpy).toHaveBeenCalledWith('Failed to persist LLM change:', expect.any(Error));
    });

    consoleErrorSpy.mockRestore();
  });

  it('should close the LLM dialog after selection', async () => {
    renderAgentNode();

    await waitFor(() => {
      expect(mockGetAgent).toHaveBeenCalledWith('agent-123');
    });

    // Open LLM dialog
    const llmBadge = screen.getByText('databricks-llama-4-maverick');
    fireEvent.click(llmBadge);
    await waitFor(() => {
      expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
    });

    // Select LLM
    fireEvent.click(screen.getByTestId('select-llm-btn'));

    // Dialog should close
    await waitFor(() => {
      expect(screen.queryByTestId('llm-dialog')).not.toBeInTheDocument();
    });
  });
});

describe('AgentNode - Basic Rendering', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetAgent.mockResolvedValue(mockAgent);
  });

  it('should render agent role', () => {
    renderAgentNode();
    expect(screen.getByText('Researcher')).toBeInTheDocument();
  });

  it('should render LLM badge with model name', () => {
    renderAgentNode();
    expect(screen.getByText('databricks-llama-4-maverick')).toBeInTheDocument();
  });

  it('should render default LLM when no llm specified', () => {
    renderAgentNode({
      data: {
        ...defaultProps.data,
        llm: undefined,
      },
    });
    // Falls back to default display
    expect(screen.getByText('databricks-llama-4-maverick')).toBeInTheDocument();
  });

  it('should render handles for connections', () => {
    renderAgentNode();
    expect(screen.getByTestId('handle-target-top')).toBeInTheDocument();
    expect(screen.getByTestId('handle-target-left')).toBeInTheDocument();
  });
});
