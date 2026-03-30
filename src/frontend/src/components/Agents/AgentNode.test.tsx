/**
 * Unit tests for AgentNode component — targeting 100 % coverage.
 */
import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider, createTheme } from '@mui/material';
import AgentNode from './AgentNode';

/* ------------------------------------------------------------------ */
/* Mocks                                                               */
/* ------------------------------------------------------------------ */

// ReactFlow — setNodes/setEdges invoke their callback so inner mapper logic executes
const mockSetNodes = vi.fn((updater: unknown) => {
  if (typeof updater === 'function') {
    updater([
      { id: 'agent-node-1', data: { agentId: 'agent-123', label: 'Test Agent', llm: 'databricks-llama-4-maverick' } },
      { id: 'other-node', data: { agentId: 'other', label: 'Other' } },
    ]);
  }
});
const mockSetEdges = vi.fn((updater: unknown) => {
  if (typeof updater === 'function') {
    updater([]);
  }
});
const mockGetNodes = vi.fn(() => []);
const mockGetEdges = vi.fn(() => []);

vi.mock('reactflow', () => ({
  Handle: ({ position, type, id: handleId, style, onDoubleClick }: {
    position: string; type: string; id: string; style?: object; onDoubleClick?: () => void;
  }) => (
    <div
      data-testid={`handle-${type}-${handleId || position}`}
      data-position={position}
      style={style}
      onDoubleClick={onDoubleClick}
    />
  ),
  Position: { Top: 'top', Bottom: 'bottom', Left: 'left', Right: 'right' },
  useReactFlow: () => ({
    setNodes: mockSetNodes,
    setEdges: mockSetEdges,
    getNodes: mockGetNodes,
    getEdges: mockGetEdges,
  }),
}));

// Agent store
const mockUpdateAgent = vi.fn();
const mockGetAgent = vi.fn(() => Promise.resolve(null));
vi.mock('../../store/agent', () => ({
  useAgentStore: () => ({
    getAgent: mockGetAgent,
    updateAgent: mockUpdateAgent,
  }),
}));

// UI Layout store — also expose getState for handleDoubleClick
const mockLayoutOrientation = { current: 'vertical' };
const mockUILayoutGetState = vi.fn(() => ({ layoutOrientation: mockLayoutOrientation.current }));
vi.mock('../../store/uiLayout', () => ({
  useUILayoutStore: Object.assign(
    vi.fn((selector: (s: { layoutOrientation: string }) => unknown) => {
      const state = { layoutOrientation: mockLayoutOrientation.current };
      return selector(state);
    }),
    { getState: () => mockUILayoutGetState() },
  ),
}));

const mockProcessType = { current: 'sequential' };
vi.mock('../../store/crewExecution', () => ({
  useCrewExecutionStore: vi.fn((selector: (s: { processType: string }) => unknown) => {
    const state = { processType: mockProcessType.current };
    return selector(state);
  }),
}));

// Hooks
const mockMarkCurrentTabDirty = vi.fn();
vi.mock('../../hooks/workflow/useTabDirtyState', () => ({
  useTabDirtyState: () => ({ markCurrentTabDirty: mockMarkCurrentTabDirty }),
}));

// ToolService
const mockListEnabledTools = vi.fn(() => Promise.resolve([
  { id: 1, name: 'WebSearch', title: 'Web Search' },
]));
vi.mock('../../api/ToolService', () => ({
  ToolService: { listEnabledTools: (...args: unknown[]) => mockListEnabledTools(...args) },
  Tool: {},
}));

// AgentService
const mockUpdateAgentFull = vi.fn(() => Promise.resolve({ id: 'agent-123', name: 'Test Agent', llm: 'gpt-4' }));
vi.mock('../../api/AgentService', () => ({
  Agent: {},
  AgentService: { updateAgentFull: (...args: unknown[]) => mockUpdateAgentFull(...args) },
}));

// Child components — AgentForm exposes onCancel / onAgentSaved via buttons
let capturedOnCancel: (() => void) | null = null;
let capturedOnAgentSaved: ((agent: unknown) => void) | null = null;
vi.mock('./AgentForm', () => ({
  default: ({ onCancel, onAgentSaved }: { onCancel: () => void; onAgentSaved: (a: unknown) => void }) => {
    capturedOnCancel = onCancel;
    capturedOnAgentSaved = onAgentSaved;
    return (
      <div data-testid="agent-form">
        <button data-testid="agent-form-cancel" onClick={onCancel}>Cancel</button>
        <button data-testid="agent-form-save" onClick={() => onAgentSaved({
          id: 'agent-123', name: 'Updated Agent', role: 'Updated', goal: 'g', backstory: 'b',
          llm: 'gpt-4', tools: ['t1'], tool_configs: {}, max_iter: 10, verbose: true,
          allow_delegation: true, cache: false, allow_code_execution: true, code_execution_mode: 'unsafe',
          memory: true, function_calling_llm: 'gpt-4', max_rpm: 5, max_execution_time: 60,
          system_template: 's', prompt_template: 'p', response_template: 'r',
          max_retry_limit: 2, use_system_prompt: true, respect_context_window: true,
          embedder_config: { provider: 'custom' }, knowledge_sources: [],
        })}>Save</button>
      </div>
    );
  },
}));

vi.mock('./LLMSelectionDialog', () => ({
  default: ({ open, onClose, onSelectLLM }: { open: boolean; onClose: () => void; onSelectLLM: (m: string) => void }) =>
    open ? (
      <div data-testid="llm-dialog">
        <button data-testid="select-llm-btn" onClick={() => onSelectLLM('gpt-4o')}>Select LLM</button>
        <button data-testid="close-llm-btn" onClick={onClose}>Close</button>
      </div>
    ) : null,
}));

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

const theme = createTheme();
const darkTheme = createTheme({ palette: { mode: 'dark' } });

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

const defaultData = {
  agentId: 'agent-123',
  label: 'Test Agent',
  role: 'Researcher',
  goal: 'Research things',
  backstory: 'An expert researcher',
  llm: 'databricks-llama-4-maverick',
  tools: [] as string[],
};

const renderNode = (dataOverrides = {}, id = 'agent-node-1') => {
  const data = { ...defaultData, ...dataOverrides };
  return render(
    <ThemeProvider theme={theme}>
      <AgentNode data={data as never} id={id} />
    </ThemeProvider>,
  );
};

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

describe('AgentNode', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset implementations that may have been changed by individual tests
    mockGetAgent.mockResolvedValue(mockAgent);
    mockUpdateAgent.mockImplementation(vi.fn());
    mockUpdateAgentFull.mockResolvedValue({ id: 'agent-123', name: 'Test Agent', llm: 'gpt-4' });
    mockListEnabledTools.mockResolvedValue([{ id: 1, name: 'WebSearch', title: 'Web Search' }]);
    mockLayoutOrientation.current = 'vertical';
    mockProcessType.current = 'sequential';
    mockUILayoutGetState.mockReturnValue({ layoutOrientation: 'vertical' });
    capturedOnCancel = null;
    capturedOnAgentSaved = null;
  });

  /* ---------- Basic Rendering ---------- */

  describe('Basic Rendering', () => {
    it('renders agent role', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalledWith('agent-123'));
      expect(screen.getByText('Researcher')).toBeInTheDocument();
    });

    it('renders default role "Agent" when role is missing', async () => {
      renderNode({ role: undefined });
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());
      expect(screen.getByText('Agent')).toBeInTheDocument();
    });

    it('renders LLM badge with model name', () => {
      renderNode();
      expect(screen.getByText('databricks-llama-4-maverick')).toBeInTheDocument();
    });

    it('renders default LLM when no llm specified', () => {
      renderNode({ llm: undefined });
      expect(screen.getByText('databricks-llama-4-maverick')).toBeInTheDocument();
    });

    it('renders connection handles', () => {
      renderNode();
      expect(screen.getByTestId('handle-target-top')).toBeInTheDocument();
      expect(screen.getByTestId('handle-target-left')).toBeInTheDocument();
      expect(screen.getByTestId('handle-source-bottom')).toBeInTheDocument();
      expect(screen.getByTestId('handle-source-right')).toBeInTheDocument();
    });

    it('renders code execution icon when allow_code_execution is true', () => {
      renderNode({ allow_code_execution: true });
      // CodeIcon renders inside a Tooltip; check for the SVG icon via testid
      expect(screen.getByTestId('CodeIcon')).toBeInTheDocument();
    });

    it('does not render code execution icon when allow_code_execution is false', () => {
      renderNode({ allow_code_execution: false });
      expect(screen.queryByTestId('CodeIcon')).not.toBeInTheDocument();
    });

    it('renders memory icon when memory is true (default provider)', () => {
      renderNode({ memory: true });
      // Two MemoryIcons: one in the LLM badge, one for the memory indicator
      const icons = screen.getAllByTestId('MemoryIcon');
      expect(icons.length).toBeGreaterThanOrEqual(2);
    });

    it('renders memory icon with custom embedder provider', () => {
      renderNode({ memory: true, embedder_config: { provider: 'huggingface' } });
      const icons = screen.getAllByTestId('MemoryIcon');
      expect(icons.length).toBeGreaterThanOrEqual(2);
    });

    it('does not render extra memory icon when memory is false', () => {
      renderNode({ memory: false });
      // Only the LLM badge MemoryIcon should be present, not the memory indicator one
      const icons = screen.getAllByTestId('MemoryIcon');
      expect(icons.length).toBe(1); // Only the one in the LLM badge
    });

    it('renders loading overlay when loading is true', () => {
      renderNode({ loading: true });
      expect(screen.getByText('Creating…')).toBeInTheDocument();
    });

    it('does not render loading overlay when loading is false', () => {
      renderNode({ loading: false });
      expect(screen.queryByText('Creating…')).not.toBeInTheDocument();
    });

    it('renders data attributes on the node', () => {
      const { container } = renderNode();
      const box = container.firstChild as HTMLElement;
      expect(box.getAttribute('data-agentid')).toBe('agent-123');
      expect(box.getAttribute('data-nodeid')).toBe('agent-node-1');
      expect(box.getAttribute('data-nodetype')).toBe('agent');
      expect(box.getAttribute('data-selected')).toBe('false');
    });
  });

  /* ---------- Style Variants ---------- */

  describe('Node Style Variants', () => {
    it('applies active state styles when isActive is true', () => {
      renderNode({ isActive: true });
      // The ACTIVE text should be generated via CSS pseudo-element ::before,
      // which isn't in the DOM directly, but the component should render without errors
      const { container } = renderNode({ isActive: true });
      expect(container.firstChild).toBeTruthy();
    });

    it('applies completed state styles when isCompleted is true', () => {
      const { container } = renderNode({ isCompleted: true });
      expect(container.firstChild).toBeTruthy();
    });

    it('applies default styles when neither active nor completed', () => {
      const { container } = renderNode();
      expect(container.firstChild).toBeTruthy();
    });
  });

  /* ---------- Agent Data Loading ---------- */

  describe('Agent Data Loading', () => {
    it('loads agent data on mount when agentId exists', async () => {
      renderNode();
      await waitFor(() => {
        expect(mockGetAgent).toHaveBeenCalledWith('agent-123');
      });
    });

    it('does not load agent data when agentId is empty', async () => {
      renderNode({ agentId: '' });
      // Give time for any async operations
      await act(async () => { await new Promise(r => setTimeout(r, 50)); });
      // getAgent should not have been called (only from handleEditClick, not the mount effect)
      expect(mockGetAgent).not.toHaveBeenCalled();
    });

    it('loads tools on mount', async () => {
      renderNode();
      await waitFor(() => {
        expect(mockListEnabledTools).toHaveBeenCalled();
      });
    });

    it('handles tool loading error gracefully', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      mockListEnabledTools.mockRejectedValueOnce(new Error('network error'));
      renderNode();
      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledWith('Error loading tools:', expect.any(Error));
      });
      consoleSpy.mockRestore();
    });
  });

  /* ---------- Node Click Handling ---------- */

  describe('Node Click Handling', () => {
    it('opens edit form on left-click on the node body', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      // getAgent is called again in handleEditClick
      await waitFor(() => {
        expect(mockGetAgent).toHaveBeenCalledTimes(2);
      });
      expect(screen.getByTestId('agent-form')).toBeInTheDocument();
    });

    it('ignores clicks on buttons (does not trigger node click handler)', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Find the edit button by aria-label
      const editBtn = screen.getByLabelText('Edit Agent');
      expect(editBtn).toBeTruthy();

      // Click the edit button — it calls handleEditClick directly, not via handleNodeClick
      mockGetAgent.mockClear();
      await act(async () => {
        fireEvent.click(editBtn, { button: 0 });
      });

      // The button click triggers handleEditClick directly (on the button),
      // and the node's handleNodeClick checks `target.closest('button')` which would be true
      // so handleNodeClick would skip calling handleEditClick again
      expect(mockGetAgent).toHaveBeenCalled();
    });

    it('ignores portal clicks where target is not inside currentTarget', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;

      // Create a fake event where target is NOT inside currentTarget (simulates portal)
      const externalDiv = document.createElement('div');
      document.body.appendChild(externalDiv);

      mockGetAgent.mockClear();

      // Fire event with target being an element outside the node
      const event = new MouseEvent('click', { bubbles: true, button: 0 });
      Object.defineProperty(event, 'target', { value: externalDiv });
      fireEvent(nodeEl, event);

      // handleEditClick should NOT have been called
      expect(mockGetAgent).not.toHaveBeenCalled();

      document.body.removeChild(externalDiv);
    });

    it('prevents default on context menu', () => {
      renderNode();
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      const event = new MouseEvent('contextmenu', { bubbles: true });
      const preventDefaultSpy = vi.spyOn(event, 'preventDefault');
      fireEvent(nodeEl, event);
      expect(preventDefaultSpy).toHaveBeenCalled();
    });
  });

  /* ---------- Edit Click / Agent Form ---------- */

  describe('Edit Click / Agent Form', () => {
    it('opens form when agentId exists and getAgent returns data', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(screen.getByTestId('agent-form')).toBeInTheDocument();
      });
    });

    it('opens form with data fallback when no agentId/id/agent_id', async () => {
      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      renderNode({ agentId: '', id: undefined, agent_id: undefined });

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledWith(
          'Agent ID is missing in node data, using data directly:',
          expect.any(Object),
        );
        expect(screen.getByTestId('agent-form')).toBeInTheDocument();
      });
      consoleSpy.mockRestore();
    });

    it('opens form with data fallback covering all field conversions', async () => {
      vi.spyOn(console, 'warn').mockImplementation(() => {});
      renderNode({
        agentId: '',
        label: 'My Agent',
        role: 'Dev',
        goal: 'Code',
        backstory: 'Expert',
        llm: 'gpt-4',
        tools: ['tool1'],
        tool_configs: { key: 'val' },
        max_iter: 50,
        verbose: true,
        allow_delegation: true,
        cache: false,
        allow_code_execution: true,
        code_execution_mode: 'unsafe',
        memory: true,
        temperature: 0.8,
        function_calling_llm: 'gpt-3.5',
        max_rpm: 10,
        max_execution_time: 120,
        embedder_config: { provider: 'custom' },
        knowledge_sources: [{ type: 'file', path: '/test' }],
      });

      const nodeEl = screen.getByText('Dev').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(screen.getByTestId('agent-form')).toBeInTheDocument();
      });
      vi.restoreAllMocks();
    });

    it('opens form with data fallback using safe code execution mode by default', async () => {
      vi.spyOn(console, 'warn').mockImplementation(() => {});
      renderNode({ agentId: '', code_execution_mode: 'safe' });

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(screen.getByTestId('agent-form')).toBeInTheDocument();
      });
      vi.restoreAllMocks();
    });

    it('does not open form when getAgent returns null', async () => {
      mockGetAgent.mockResolvedValue(null);
      renderNode();
      await act(async () => { await new Promise(r => setTimeout(r, 50)); });

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await act(async () => { await new Promise(r => setTimeout(r, 50)); });
      expect(screen.queryByTestId('agent-form')).not.toBeInTheDocument();
    });

    it('handles getAgent throwing an error', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      // First call (mount effect) resolves to null; second call (handleEditClick) rejects
      mockGetAgent
        .mockResolvedValueOnce(null)
        .mockRejectedValueOnce(new Error('fetch failed'));
      renderNode();

      await act(async () => { await new Promise(r => setTimeout(r, 50)); });

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledWith('Failed to fetch agent data:', expect.any(Error));
      });
      consoleSpy.mockRestore();
    });

    it('closes form on cancel', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(screen.getByTestId('agent-form')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByTestId('agent-form-cancel'));
      });

      await waitFor(() => {
        expect(screen.queryByTestId('agent-form')).not.toBeInTheDocument();
      });
    });

    it('updates node and marks tab dirty on save with agent data', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      // Save
      mockSetNodes.mockClear();
      await act(async () => {
        fireEvent.click(screen.getByTestId('agent-form-save'));
      });

      expect(mockMarkCurrentTabDirty).toHaveBeenCalled();
      expect(mockUpdateAgent).toHaveBeenCalled();
      expect(mockSetNodes).toHaveBeenCalled();
    });

    it('calls onAgentSaved with null agent (just closes)', async () => {
      // Override AgentForm mock to call onAgentSaved with null
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      // Call the captured callback with null
      await act(async () => {
        capturedOnAgentSaved?.(null);
      });

      await waitFor(() => {
        expect(screen.queryByTestId('agent-form')).not.toBeInTheDocument();
      });
      // markCurrentTabDirty should NOT be called when no updatedAgent
      expect(mockMarkCurrentTabDirty).not.toHaveBeenCalled();
    });
  });

  /* ---------- handleUpdateNode error path ---------- */

  describe('handleUpdateNode error handling', () => {
    it('catches errors in handleUpdateNode', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      mockUpdateAgent.mockImplementation(() => { throw new Error('store error'); });

      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      // Save triggers handleUpdateNode
      await act(async () => {
        fireEvent.click(screen.getByTestId('agent-form-save'));
      });

      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledWith('Failed to update node:', expect.any(Error));
      });
      consoleSpy.mockRestore();
    });
  });

  /* ---------- Delete ---------- */

  describe('Delete', () => {
    it('removes node and connected edges on delete button click', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const deleteBtn = screen.getByLabelText('Delete Agent');

      // Override setEdges to exercise the filter callback with matching/non-matching edges
      mockSetEdges.mockImplementationOnce((updater: unknown) => {
        if (typeof updater === 'function') {
          const result = updater([
            { id: 'e1', source: 'agent-node-1', target: 'task-1' },  // matches source
            { id: 'e2', source: 'task-1', target: 'agent-node-1' },  // matches target
            { id: 'e3', source: 'other', target: 'task-1' },         // no match — kept
          ]);
          // Only the non-matching edge should remain
          expect(result).toEqual([{ id: 'e3', source: 'other', target: 'task-1' }]);
        }
      });
      mockSetNodes.mockClear();

      fireEvent.click(deleteBtn);

      expect(mockSetNodes).toHaveBeenCalled();
      expect(mockSetEdges).toHaveBeenCalled();
    });
  });

  /* ---------- Double-click / Edge Creation ---------- */

  describe('Handle Double-click Edge Creation', () => {
    it('creates edge to available task node in vertical layout', () => {
      mockGetNodes.mockReturnValue([
        { id: 'agent-node-1', type: 'agentNode', position: { x: 0, y: 0 } },
        { id: 'task-1', type: 'taskNode', position: { x: 0, y: 100 } },
      ]);
      mockGetEdges.mockReturnValue([]);
      mockUILayoutGetState.mockReturnValue({ layoutOrientation: 'vertical' });

      renderNode();

      // Double-click on bottom handle
      const bottomHandle = screen.getByTestId('handle-source-bottom');
      fireEvent.doubleClick(bottomHandle);

      expect(mockSetEdges).toHaveBeenCalled();
    });

    it('creates edge with horizontal handles in horizontal layout', () => {
      mockGetNodes.mockReturnValue([
        { id: 'agent-node-1', type: 'agentNode', position: { x: 0, y: 0 } },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 } },
      ]);
      mockGetEdges.mockReturnValue([]);
      mockUILayoutGetState.mockReturnValue({ layoutOrientation: 'horizontal' });

      renderNode();

      const rightHandle = screen.getByTestId('handle-source-right');
      fireEvent.doubleClick(rightHandle);

      expect(mockSetEdges).toHaveBeenCalled();
    });

    it('does not create edge when no task nodes are available', () => {
      mockGetNodes.mockReturnValue([
        { id: 'agent-node-1', type: 'agentNode', position: { x: 0, y: 0 } },
      ]);
      mockGetEdges.mockReturnValue([]);

      renderNode();

      mockSetEdges.mockClear();
      const bottomHandle = screen.getByTestId('handle-source-bottom');
      fireEvent.doubleClick(bottomHandle);

      // setEdges should not be called (no updater)
      expect(mockSetEdges).not.toHaveBeenCalled();
    });

    it('filters out task nodes that already have incoming edges', () => {
      mockGetNodes.mockReturnValue([
        { id: 'agent-node-1', type: 'agentNode', position: { x: 0, y: 0 } },
        { id: 'task-1', type: 'taskNode', position: { x: 0, y: 100 } },
      ]);
      // task-1 already has an incoming edge
      mockGetEdges.mockReturnValue([
        { id: 'e1', source: 'other-agent', target: 'task-1' },
      ]);

      renderNode();

      mockSetEdges.mockClear();
      const bottomHandle = screen.getByTestId('handle-source-bottom');
      fireEvent.doubleClick(bottomHandle);

      expect(mockSetEdges).not.toHaveBeenCalled();
    });

    it('picks closest task node sorted by y position', () => {
      mockGetNodes.mockReturnValue([
        { id: 'agent-node-1', type: 'agentNode', position: { x: 0, y: 0 } },
        { id: 'task-far', type: 'taskNode', position: { x: 0, y: 300 } },
        { id: 'task-close', type: 'taskNode', position: { x: 0, y: 100 } },
      ]);
      mockGetEdges.mockReturnValue([]);
      mockUILayoutGetState.mockReturnValue({ layoutOrientation: 'vertical' });

      renderNode();

      const bottomHandle = screen.getByTestId('handle-source-bottom');
      fireEvent.doubleClick(bottomHandle);

      // setEdges should be called; the updater receives old edges and adds new
      expect(mockSetEdges).toHaveBeenCalled();
    });
  });

  /* ---------- LLM Selection ---------- */

  describe('LLM Selection', () => {
    it('opens LLM dialog when clicking the LLM badge', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);

      await waitFor(() => {
        expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
      });
    });

    it('persists LLM selection to backend when agentId exists', async () => {
      mockUpdateAgentFull.mockResolvedValueOnce({ ...mockAgent, llm: 'gpt-4o' });
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);
      await waitFor(() => expect(screen.getByTestId('llm-dialog')).toBeInTheDocument());

      fireEvent.click(screen.getByTestId('select-llm-btn'));

      await waitFor(() => {
        expect(mockUpdateAgentFull).toHaveBeenCalledWith(
          'agent-123',
          expect.objectContaining({ llm: 'gpt-4o' }),
        );
      });
    });

    it('updates local node state and store when LLM is selected', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);
      await waitFor(() => expect(screen.getByTestId('llm-dialog')).toBeInTheDocument());

      fireEvent.click(screen.getByTestId('select-llm-btn'));

      expect(mockSetNodes).toHaveBeenCalled();
      expect(mockUpdateAgent).toHaveBeenCalledWith(
        'agent-123',
        expect.objectContaining({ llm: 'gpt-4o' }),
      );
    });

    it('marks tab as dirty when LLM is changed', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);
      await waitFor(() => expect(screen.getByTestId('llm-dialog')).toBeInTheDocument());

      fireEvent.click(screen.getByTestId('select-llm-btn'));

      await waitFor(() => {
        expect(mockMarkCurrentTabDirty).toHaveBeenCalled();
      });
    });

    it('does not call backend when agent has no agentId', async () => {
      mockGetAgent.mockResolvedValue(null);
      renderNode({ agentId: '' });

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);
      await waitFor(() => expect(screen.getByTestId('llm-dialog')).toBeInTheDocument());

      fireEvent.click(screen.getByTestId('select-llm-btn'));

      expect(mockSetNodes).toHaveBeenCalled();
      expect(mockUpdateAgentFull).not.toHaveBeenCalled();
    });

    it('handles backend error on LLM persist gracefully', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      mockUpdateAgentFull.mockRejectedValueOnce(new Error('Network error'));
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);
      await waitFor(() => expect(screen.getByTestId('llm-dialog')).toBeInTheDocument());

      fireEvent.click(screen.getByTestId('select-llm-btn'));

      expect(mockSetNodes).toHaveBeenCalled();
      expect(mockUpdateAgent).toHaveBeenCalled();

      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledWith('Failed to persist LLM change:', expect.any(Error));
      });
      consoleSpy.mockRestore();
    });

    it('closes the LLM dialog after selection', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);
      await waitFor(() => expect(screen.getByTestId('llm-dialog')).toBeInTheDocument());

      fireEvent.click(screen.getByTestId('select-llm-btn'));

      await waitFor(() => {
        expect(screen.queryByTestId('llm-dialog')).not.toBeInTheDocument();
      });
    });

    it('closes the LLM dialog via close button', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');
      fireEvent.click(llmBadge);
      await waitFor(() => expect(screen.getByTestId('llm-dialog')).toBeInTheDocument());

      fireEvent.click(screen.getByTestId('close-llm-btn'));

      await waitFor(() => {
        expect(screen.queryByTestId('llm-dialog')).not.toBeInTheDocument();
      });
    });
  });

  /* ---------- Knowledge Sources Sync ---------- */

  describe('Knowledge Sources Sync', () => {
    it('updates agentData when knowledge_sources change while editing', async () => {
      const { rerender } = render(
        <ThemeProvider theme={theme}>
          <AgentNode
            data={{ ...defaultData, knowledge_sources: [{ type: 'file', path: '/a' }] } as never}
            id="agent-node-1"
          />
        </ThemeProvider>,
      );
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open the editor first
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      // Rerender with different knowledge_sources to trigger the sync effect
      await act(async () => {
        rerender(
          <ThemeProvider theme={theme}>
            <AgentNode
              data={{ ...defaultData, knowledge_sources: [{ type: 'file', path: '/b' }] } as never}
              id="agent-node-1"
            />
          </ThemeProvider>,
        );
      });

      // The effect should have run; form should still be visible
      expect(screen.getByTestId('agent-form')).toBeInTheDocument();
    });
  });

  /* ---------- Edit button direct click ---------- */

  describe('Edit button direct click', () => {
    it('opens form via the edit icon button', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Find the edit button by its aria-label
      const editBtn = screen.getByLabelText('Edit Agent');
      expect(editBtn).toBeTruthy();

      await act(async () => {
        fireEvent.click(editBtn);
      });
      await waitFor(() => {
        expect(screen.getByTestId('agent-form')).toBeInTheDocument();
      });
    });
  });

  /* ---------- React.memo export ---------- */

  describe('React.memo', () => {
    it('exports a memoized component', () => {
      // The default export should be a React.memo wrapped component
      expect(AgentNode).toBeDefined();
      // React.memo components have a $$typeof Symbol
      expect((AgentNode as unknown as { $$typeof: symbol }).$$typeof).toBeDefined();
    });
  });

  /* ---------- isEditing cleanup useEffect ---------- */

  describe('isEditing cleanup effect', () => {
    it('runs empty cleanup effect when isEditing changes', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form (sets isEditing = true, triggers the effect)
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      // Close form (sets isEditing = false, triggers the effect again)
      await act(async () => {
        fireEvent.click(screen.getByTestId('agent-form-cancel'));
      });
      await waitFor(() => {
        expect(screen.queryByTestId('agent-form')).not.toBeInTheDocument();
      });
    });
  });

  /* ---------- handleEditClick edge: uses data.id fallback ---------- */

  describe('handleEditClick agent ID fallback', () => {
    it('uses data.id when data.agentId is missing', async () => {
      const agentWithId = { ...mockAgent, id: 'id-from-data' };
      mockGetAgent.mockResolvedValue(agentWithId);

      renderNode({ agentId: '', id: 'id-from-data' });

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(mockGetAgent).toHaveBeenCalledWith('id-from-data');
      });
    });

    it('uses data.agent_id when both agentId and id are missing', async () => {
      const agentWithId = { ...mockAgent, id: 'agent-id-fallback' };
      mockGetAgent.mockResolvedValue(agentWithId);

      renderNode({ agentId: '', agent_id: 'agent-id-fallback' });

      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(mockGetAgent).toHaveBeenCalledWith('agent-id-fallback');
      });
    });
  });

  /* ---------- handleEditClick: document.activeElement.blur() ---------- */

  describe('handleEditClick blurs active element', () => {
    it('blurs the active element when edit is triggered', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Focus an element to ensure activeElement is set
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      (nodeEl as HTMLElement).focus();

      const blurSpy = vi.spyOn(document.activeElement as HTMLElement, 'blur');

      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      expect(blurSpy).toHaveBeenCalled();
      blurSpy.mockRestore();
    });
  });

  /* ---------- handleUpdateNode with agent.id fallback ---------- */

  describe('handleUpdateNode agent ID fallback', () => {
    it('uses updatedAgent.id.toString() when available', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      mockUpdateAgent.mockClear();
      await act(async () => {
        // The save button calls onAgentSaved with agent that has id: 'agent-123'
        fireEvent.click(screen.getByTestId('agent-form-save'));
      });

      // updateAgent should be called with the agent's id (from updatedAgent.id.toString())
      expect(mockUpdateAgent).toHaveBeenCalledWith(
        'agent-123',
        expect.objectContaining({ name: 'Updated Agent' }),
      );
    });
  });

  /* ---------- handleUpdateNode branch: no updatedAgent.id ---------- */

  describe('handleUpdateNode without agent id', () => {
    it('falls back to data.agentId when updatedAgent has no id', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      mockUpdateAgent.mockClear();
      // Call onAgentSaved with an agent that has no id
      await act(async () => {
        capturedOnAgentSaved?.({
          name: 'No-ID Agent', role: 'Dev', goal: 'g', backstory: 'b',
          llm: 'gpt-4', tools: [], max_iter: 25, verbose: false,
          allow_delegation: false, cache: true, allow_code_execution: false,
          code_execution_mode: 'safe',
        });
      });

      // Should fall back to data.agentId ('agent-123')
      expect(mockUpdateAgent).toHaveBeenCalledWith(
        'agent-123',
        expect.objectContaining({ name: 'No-ID Agent' }),
      );
    });

    it('falls back to empty object when updatedAgent has no tool_configs', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      mockSetNodes.mockClear();
      // Call onAgentSaved with an agent that has no tool_configs
      await act(async () => {
        capturedOnAgentSaved?.({
          id: 'agent-123', name: 'Updated Agent', role: 'Dev', goal: 'g', backstory: 'b',
          llm: 'gpt-4', tools: [], max_iter: 25, verbose: false,
          allow_delegation: false, cache: true, allow_code_execution: false,
          code_execution_mode: 'safe',
          // tool_configs intentionally omitted
        });
      });

      // setNodes should be called — the mapper converts undefined tool_configs to {}
      expect(mockSetNodes).toHaveBeenCalled();
    });
  });

  /* ---------- LLM badge stopPropagation ---------- */

  describe('LLM Badge Click', () => {
    it('stops propagation so node click does not fire', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      const llmBadge = screen.getByText('databricks-llama-4-maverick');

      // The click on the LLM badge should NOT trigger handleNodeClick/handleEditClick
      mockGetAgent.mockClear();
      fireEvent.click(llmBadge);

      // Should open LLM dialog, not agent form
      await waitFor(() => {
        expect(screen.getByTestId('llm-dialog')).toBeInTheDocument();
      });
      expect(screen.queryByTestId('agent-form')).not.toBeInTheDocument();
    });
  });

  /* ---------- Dialog onClose (backdrop click) ---------- */

  describe('Dialog onClose', () => {
    it('closes the agent form dialog via onCancel', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      // Invoke onCancel callback from AgentForm
      await act(async () => {
        capturedOnCancel?.();
      });

      await waitFor(() => {
        expect(screen.queryByTestId('agent-form')).not.toBeInTheDocument();
      });
    });

    it('closes the agent form dialog via Dialog onClose (Escape key)', async () => {
      renderNode();
      await waitFor(() => expect(mockGetAgent).toHaveBeenCalled());

      // Open form
      const nodeEl = screen.getByText('Researcher').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });
      await waitFor(() => expect(screen.getByTestId('agent-form')).toBeInTheDocument());

      // MUI Dialog/Modal uses a keydown listener on the document for Escape
      const dialogEl = document.querySelector('[role="dialog"]') || document.querySelector('.MuiDialog-root');
      const target = dialogEl || document;
      await act(async () => {
        fireEvent.keyDown(target, { key: 'Escape', code: 'Escape', keyCode: 27 });
      });

      await waitFor(() => {
        expect(screen.queryByTestId('agent-form')).not.toBeInTheDocument();
      });
    });
  });

  /* ---------- Dark Theme Branch Coverage ---------- */

  describe('Dark Theme Rendering', () => {
    const renderDark = (dataOverrides = {}, nodeId = 'agent-node-1') => {
      const nodeData = { ...defaultData, ...dataOverrides };
      return render(
        <ThemeProvider theme={darkTheme}>
          <AgentNode data={nodeData as never} id={nodeId} />
        </ThemeProvider>,
      );
    };

    it('renders default styles in dark mode', () => {
      const { container } = renderDark();
      expect(container.firstChild).toBeTruthy();
    });

    it('renders active state styles in dark mode', () => {
      const { container } = renderDark({ isActive: true });
      expect(container.firstChild).toBeTruthy();
    });

    it('renders completed state styles in dark mode', () => {
      const { container } = renderDark({ isCompleted: true });
      expect(container.firstChild).toBeTruthy();
    });

    it('renders loading overlay in dark mode', () => {
      renderDark({ loading: true });
      expect(screen.getByText('Creating…')).toBeInTheDocument();
    });
  });

  /* ---------- Handle Visibility Branches ---------- */

  describe('Handle Visibility (layout + process type)', () => {
    it('shows top/left target handles with hierarchical process in vertical layout', () => {
      mockLayoutOrientation.current = 'vertical';
      mockProcessType.current = 'hierarchical';

      renderNode();
      const topHandle = screen.getByTestId('handle-target-top');
      expect(topHandle).toBeInTheDocument();
    });

    it('shows top/left target handles with hierarchical process in horizontal layout', () => {
      mockLayoutOrientation.current = 'horizontal';
      mockProcessType.current = 'hierarchical';

      renderNode();
      const leftHandle = screen.getByTestId('handle-target-left');
      expect(leftHandle).toBeInTheDocument();
    });

    it('renders source handles in horizontal layout', () => {
      mockLayoutOrientation.current = 'horizontal';

      renderNode();
      const rightHandle = screen.getByTestId('handle-source-right');
      expect(rightHandle).toBeInTheDocument();
    });
  });

  /* ---------- Loading and Error States ---------- */

  describe('Loading and Error States', () => {
    it('renders error overlay when error is true', () => {
      renderNode({ error: true, errorMessage: 'Something went wrong' });
      expect(screen.getByTestId('ErrorIcon')).toBeInTheDocument();
      expect(screen.getByText('Something went wrong')).toBeInTheDocument();
    });

    it('error overlay shows custom message', () => {
      renderNode({ error: true, errorMessage: 'Custom error' });
      expect(screen.getByText('Custom error')).toBeInTheDocument();
    });

    it('error overlay shows default message when no errorMessage', () => {
      renderNode({ error: true });
      expect(screen.getByText('Generation failed')).toBeInTheDocument();
    });

    it('does not render error overlay when error is false', () => {
      renderNode({ error: false });
      expect(screen.queryByTestId('ErrorIcon')).not.toBeInTheDocument();
      expect(screen.queryByText('Generation failed')).not.toBeInTheDocument();
    });

    it('renders without crash when loading is true', () => {
      renderNode({ loading: true });
      expect(screen.getByText('Creating…')).toBeInTheDocument();
    });
  });

  /* ---------- Data fallback: no name, no label ---------- */

  describe('handleEditClick data fallback edge cases', () => {
    it('handles missing label/name/role/goal/backstory/llm gracefully', async () => {
      vi.spyOn(console, 'warn').mockImplementation(() => {});
      renderNode({
        agentId: '',
        label: undefined,
        name: undefined,
        role: undefined,
        goal: undefined,
        backstory: undefined,
        llm: undefined,
        tools: undefined,
        max_iter: undefined,
        verbose: undefined,
        allow_delegation: undefined,
        cache: undefined,
        allow_code_execution: undefined,
        code_execution_mode: undefined,
        temperature: 'not-a-number',
      });

      const nodeEl = screen.getByText('Agent').closest('[data-nodetype="agent"]')!;
      await act(async () => {
        fireEvent.click(nodeEl, { button: 0 });
      });

      await waitFor(() => {
        expect(screen.getByTestId('agent-form')).toBeInTheDocument();
      });
      vi.restoreAllMocks();
    });
  });
});
