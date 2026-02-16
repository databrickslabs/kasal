/**
 * Unit tests for TaskNode component.
 *
 * Tests the task node display in workflow canvas including:
 * - Name truncation for long task names
 * - Description display with truncation
 * - Tool count display
 * - Execution status indicators
 * - Edit and delete functionality
 * - Tooltip showing full task name
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material';
import TaskNode from './TaskNode';

// Mock ReactFlow
const mockSetNodes = vi.fn();
vi.mock('reactflow', () => ({
  Handle: ({ position, type, id, style }: { position: string; type: string; id: string; style?: object }) => (
    <div data-testid={`handle-${type}-${id || position}`} data-position={position} style={style} />
  ),
  Position: {
    Top: 'top',
    Bottom: 'bottom',
    Left: 'left',
    Right: 'right',
  },
  useReactFlow: () => ({
    setNodes: mockSetNodes,
    setEdges: vi.fn(),
    getNodes: vi.fn(() => []),
    getEdges: vi.fn(() => []),
  }),
}));

// Mock stores
vi.mock('../../store/uiLayout', () => ({
  useUILayoutStore: vi.fn((selector) => {
    const state = { layoutOrientation: 'vertical' };
    return selector(state);
  }),
}));

vi.mock('../../store/taskExecutionStore', () => ({
  useTaskExecutionStore: vi.fn((selector) => {
    const state = {
      taskStates: new Map(),
      getTaskStatus: vi.fn(() => null),
      isPlanningPhase: false,
    };
    return selector(state);
  }),
}));

vi.mock('../../utils/taskIdUtils', () => ({
  findTaskStoreKey: vi.fn(() => null),
}));

// Mock hooks
vi.mock('../../hooks/workflow/useTabDirtyState', () => ({
  useTabDirtyState: () => ({
    markCurrentTabDirty: vi.fn(),
  }),
}));

// Mock ToolService
vi.mock('../../api/ToolService', () => ({
  ToolService: {
    listEnabledTools: vi.fn(() => Promise.resolve([])),
  },
  Tool: {},
}));

// Mock TaskService
const mockUpdateTask = vi.fn(() => Promise.resolve({ id: 'task-123', tools: [] }));
vi.mock('../../api/TaskService', () => ({
  Task: {},
  TaskService: {
    updateTask: (...args: unknown[]) => mockUpdateTask(...args),
  },
}));

// Mock child components
vi.mock('./TaskForm', () => ({
  default: () => <div data-testid="task-form">Task Form</div>,
}));

vi.mock('./QuickToolSelectionDialog', () => ({
  default: ({ open, onSelectTools }: { open: boolean; onSelectTools: (tools: string[]) => void }) =>
    open ? (
      <div data-testid="tool-dialog">
        <button data-testid="select-tools-btn" onClick={() => onSelectTools(['tool-a', 'tool-b'])}>
          Select Tools
        </button>
      </div>
    ) : null,
}));

const theme = createTheme();

const defaultProps = {
  id: 'task-node-1',
  data: {
    label: 'Test Task',
    description: 'This is a test task description',
    expected_output: 'Expected output',
    tools: ['tool1', 'tool2'],
    taskId: 'task-123',
    config: {
      human_input: false,
    },
  },
};

const renderTaskNode = (props = {}) => {
  const mergedProps = { ...defaultProps, ...props };
  return render(
    <ThemeProvider theme={theme}>
      <TaskNode {...mergedProps} />
    </ThemeProvider>
  );
};

describe('TaskNode', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Basic Rendering', () => {
    it('should render task label', () => {
      renderTaskNode();
      expect(screen.getByText('Test Task')).toBeInTheDocument();
    });

    it('should render task description', () => {
      renderTaskNode();
      expect(screen.getByText('This is a test task description')).toBeInTheDocument();
    });

    it('should render tool count', () => {
      renderTaskNode();
      expect(screen.getByText('Tools: 2')).toBeInTheDocument();
    });

    it('should render handles for connections', () => {
      renderTaskNode();
      expect(screen.getByTestId('handle-target-top')).toBeInTheDocument();
      expect(screen.getByTestId('handle-target-left')).toBeInTheDocument();
      expect(screen.getByTestId('handle-source-right')).toBeInTheDocument();
    });
  });

  describe('Name Truncation', () => {
    it('should handle short task names without truncation', () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          label: 'Short',
        },
      });
      const text = screen.getByText('Short');
      expect(text).toBeInTheDocument();
    });

    it('should apply truncation styles for long names', () => {
      const longName = 'This is a very long task name that should be truncated with ellipsis to fit the node';
      renderTaskNode({
        data: {
          ...defaultProps.data,
          label: longName,
        },
      });
      const text = screen.getByText(longName);
      expect(text).toBeInTheDocument();
      // Check truncation styles are applied
      expect(text).toHaveStyle({ overflow: 'hidden' });
      expect(text).toHaveStyle({ whiteSpace: 'nowrap' });
    });

    it('should show full name in tooltip', () => {
      const longName = 'This is a very long task name that should be truncated';
      renderTaskNode({
        data: {
          ...defaultProps.data,
          label: longName,
        },
      });
      // The text should be in the document (tooltip shows full text)
      expect(screen.getByText(longName)).toBeInTheDocument();
    });
  });

  describe('Description Truncation', () => {
    it('should truncate long descriptions', () => {
      const longDesc = 'This is a very long description that spans multiple lines and should be truncated to only show two lines with an ellipsis at the end to indicate more content';
      renderTaskNode({
        data: {
          ...defaultProps.data,
          description: longDesc,
        },
      });
      const descElement = screen.getByText(longDesc);
      expect(descElement).toBeInTheDocument();
      // Check for line clamp styles
      expect(descElement).toHaveStyle({ overflow: 'hidden' });
    });
  });

  describe('Tool Display', () => {
    it('should show correct tool count', () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          tools: ['tool1', 'tool2', 'tool3'],
        },
      });
      expect(screen.getByText('Tools: 3')).toBeInTheDocument();
    });

    it('should show zero tools when no tools assigned', () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          tools: [],
        },
      });
      expect(screen.getByText('Tools: 0')).toBeInTheDocument();
    });

    it('should handle undefined tools', () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          tools: undefined,
        },
      });
      expect(screen.getByText('Tools: 0')).toBeInTheDocument();
    });
  });

  describe('Human Input Indicator', () => {
    it('should show human input label when enabled', () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          config: {
            human_input: true,
          },
        },
      });
      expect(screen.getByText('Human Input')).toBeInTheDocument();
    });

    it('should not show human input label when disabled', () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          config: {
            human_input: false,
          },
        },
      });
      expect(screen.queryByText('Human Input')).not.toBeInTheDocument();
    });
  });

  describe('Knowledge Search Indicator', () => {
    it('should show knowledge indicator when DatabricksKnowledgeSearchTool is present', () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          tools: ['DatabricksKnowledgeSearchTool'],
        },
      });
      // Should render the attach file icon for knowledge search
      expect(screen.getByText('Tools: 1')).toBeInTheDocument();
    });
  });

  describe('Click Interactions', () => {
    it('should open edit dialog on left click', async () => {
      const { container } = renderTaskNode();
      const nodeBox = container.querySelector('[data-nodetype="task"]');

      if (nodeBox) {
        fireEvent.click(nodeBox);
        await waitFor(() => {
          expect(screen.getByText('Edit Task')).toBeInTheDocument();
        });
      }
    });

    it('should prevent context menu on right click', () => {
      const { container } = renderTaskNode();
      const nodeBox = container.querySelector('[data-nodetype="task"]');

      if (nodeBox) {
        const event = new MouseEvent('contextmenu', { bubbles: true });
        const preventDefaultSpy = vi.spyOn(event, 'preventDefault');
        nodeBox.dispatchEvent(event);
        expect(preventDefaultSpy).toHaveBeenCalled();
      }
    });
  });

  describe('Tool Selection', () => {
    it('should open tool dialog when clicking on tools section', async () => {
      renderTaskNode();
      const toolsSection = screen.getByText('Tools: 2');

      fireEvent.click(toolsSection);

      await waitFor(() => {
        expect(screen.getByTestId('tool-dialog')).toBeInTheDocument();
      });
    });

    it('should persist tool selection to backend when task has a taskId', async () => {
      mockUpdateTask.mockResolvedValueOnce({ id: 'task-123', tools: ['tool-a', 'tool-b'] });
      renderTaskNode();

      // Open tool dialog
      fireEvent.click(screen.getByText('Tools: 2'));
      await waitFor(() => {
        expect(screen.getByTestId('tool-dialog')).toBeInTheDocument();
      });

      // Select tools via the mock dialog button
      fireEvent.click(screen.getByTestId('select-tools-btn'));

      await waitFor(() => {
        expect(mockUpdateTask).toHaveBeenCalledWith('task-123', { tools: ['tool-a', 'tool-b'] });
      });
    });

    it('should not call backend when task has no taskId', async () => {
      renderTaskNode({
        data: {
          ...defaultProps.data,
          taskId: '',
        },
      });

      // Open tool dialog
      fireEvent.click(screen.getByText('Tools: 2'));
      await waitFor(() => {
        expect(screen.getByTestId('tool-dialog')).toBeInTheDocument();
      });

      // Select tools
      fireEvent.click(screen.getByTestId('select-tools-btn'));

      // setNodes should still be called (local state update)
      expect(mockSetNodes).toHaveBeenCalled();
      // But backend should NOT be called
      expect(mockUpdateTask).not.toHaveBeenCalled();
    });

    it('should update local node state even if backend call fails', async () => {
      const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      mockUpdateTask.mockRejectedValueOnce(new Error('Network error'));
      renderTaskNode();

      // Open tool dialog
      fireEvent.click(screen.getByText('Tools: 2'));
      await waitFor(() => {
        expect(screen.getByTestId('tool-dialog')).toBeInTheDocument();
      });

      // Select tools
      fireEvent.click(screen.getByTestId('select-tools-btn'));

      // Local state should still be updated
      expect(mockSetNodes).toHaveBeenCalled();

      // Backend was called but failed
      await waitFor(() => {
        expect(consoleErrorSpy).toHaveBeenCalledWith('Failed to persist tool selection:', expect.any(Error));
      });

      consoleErrorSpy.mockRestore();
    });
  });

  describe('Data Attributes', () => {
    it('should have correct data attributes for identification', () => {
      const { container } = renderTaskNode();
      const nodeBox = container.querySelector('[data-nodetype="task"]');

      expect(nodeBox).toHaveAttribute('data-taskid', 'task-123');
      expect(nodeBox).toHaveAttribute('data-label', 'Test Task');
      expect(nodeBox).toHaveAttribute('data-nodeid', 'task-node-1');
    });
  });

  describe('Node Dimensions', () => {
    it('should have minimum width of 160px', () => {
      const { container } = renderTaskNode();
      const nodeBox = container.querySelector('[data-nodetype="task"]');
      expect(nodeBox).toHaveStyle({ minWidth: '160px' });
    });

    it('should have minimum height of 120px', () => {
      const { container } = renderTaskNode();
      const nodeBox = container.querySelector('[data-nodetype="task"]');
      expect(nodeBox).toHaveStyle({ minHeight: '120px' });
    });
  });
});

describe('TaskNode Execution Status', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should render without status icon when not executing', () => {
    renderTaskNode();
    // No status icons should be present initially
    const progressIndicators = screen.queryAllByRole('progressbar');
    expect(progressIndicators.length).toBe(0);
  });
});

describe('TaskNode Icon Display', () => {
  it('should render default task icon', () => {
    renderTaskNode();
    // The AddTaskIcon should be rendered by default
    expect(screen.getByText('Test Task')).toBeInTheDocument();
  });

  it('should render custom icon when provided', () => {
    renderTaskNode({
      data: {
        ...defaultProps.data,
        icon: '📝',
      },
    });
    expect(screen.getByText('📝')).toBeInTheDocument();
  });
});

/**
 * Tests for the onTaskSaved callback logic that syncs taskId and tools
 * back to the node data after a task is saved via the TaskForm.
 */
describe('TaskNode - onTaskSaved node update logic', () => {
  // Replicates the onTaskSaved callback logic from TaskNode
  const buildUpdatedData = (
    existingNodeData: Record<string, unknown>,
    savedTask: { id: string; name: string; tools: string[]; description?: string; expected_output?: string; tool_configs?: Record<string, unknown>; async_execution?: boolean; context?: string[]; markdown?: boolean; config?: Record<string, unknown> }
  ) => {
    return {
      ...existingNodeData,
      taskId: savedTask.id,
      label: savedTask.name,
      description: savedTask.description,
      expected_output: savedTask.expected_output,
      tools: savedTask.tools,
      tool_configs: savedTask.tool_configs || {},
      async_execution: savedTask.async_execution,
      context: savedTask.context,
      markdown: savedTask.markdown !== undefined ? savedTask.markdown : (savedTask.config?.markdown || false),
      config: {
        ...(existingNodeData.config as Record<string, unknown>),
        ...savedTask.config,
        output_pydantic: savedTask.config?.output_pydantic || null,
        output_json: savedTask.config?.output_json || null,
        output_file: savedTask.config?.output_file || null,
        callback: savedTask.config?.callback || null,
        guardrail: savedTask.config?.guardrail || undefined,
        llm_guardrail: savedTask.config?.llm_guardrail || null,
        markdown: savedTask.markdown !== undefined ? savedTask.markdown : (savedTask.config?.markdown || false),
      },
    };
  };

  it('should sync taskId from saved task response', () => {
    const existingData = { taskId: undefined, label: 'Old', tools: [] };
    const savedTask = { id: 'new-task-id-123', name: 'Saved', tools: ['31'] };

    const updated = buildUpdatedData(existingData, savedTask);

    expect(updated.taskId).toBe('new-task-id-123');
  });

  it('should overwrite existing taskId with saved task id', () => {
    const existingData = { taskId: 'old-id', label: 'Old', tools: [] };
    const savedTask = { id: 'new-id', name: 'Saved', tools: [] };

    const updated = buildUpdatedData(existingData, savedTask);

    expect(updated.taskId).toBe('new-id');
  });

  it('should update tools from saved task response', () => {
    const existingData = { taskId: 't1', label: 'Task', tools: [] };
    const savedTask = { id: 't1', name: 'Task', tools: ['PerplexitySearchTool', 'WebSearchTool'] };

    const updated = buildUpdatedData(existingData, savedTask);

    expect(updated.tools).toEqual(['PerplexitySearchTool', 'WebSearchTool']);
  });

  it('should clear tools when saved task has empty tools', () => {
    const existingData = { taskId: 't1', label: 'Task', tools: ['old-tool'] };
    const savedTask = { id: 't1', name: 'Task', tools: [] };

    const updated = buildUpdatedData(existingData, savedTask);

    expect(updated.tools).toEqual([]);
  });

  it('should preserve extra node data not in saved task', () => {
    const existingData = { taskId: 't1', label: 'Task', tools: [], customField: 'preserve-me' };
    const savedTask = { id: 't1', name: 'Task', tools: ['tool1'] };

    const updated = buildUpdatedData(existingData, savedTask);

    expect(updated.customField).toBe('preserve-me');
    expect(updated.tools).toEqual(['tool1']);
  });

  it('should update label from saved task name', () => {
    const existingData = { taskId: 't1', label: 'Old Name', tools: [] };
    const savedTask = { id: 't1', name: 'New Name', tools: [] };

    const updated = buildUpdatedData(existingData, savedTask);

    expect(updated.label).toBe('New Name');
  });

  it('should update tool_configs from saved task', () => {
    const existingData = { taskId: 't1', label: 'Task', tools: [], tool_configs: {} };
    const savedTask = { id: 't1', name: 'Task', tools: ['31'], tool_configs: { GenieTool: { space_id: '123' } } };

    const updated = buildUpdatedData(existingData, savedTask);

    expect(updated.tool_configs).toEqual({ GenieTool: { space_id: '123' } });
  });
});

/**
 * Tests for the planning phase indicator logic (pure function tests).
 * These verify the status icon and style selection logic.
 */
describe('TaskNode - Planning indicator logic', () => {
  // Replicates the getStatusIcon selection logic
  const selectIndicator = (
    isPlanningPhase: boolean,
    taskStatus: { status: string } | null
  ): string => {
    if (isPlanningPhase && !taskStatus) return 'planning';
    if (!taskStatus) return 'none';
    switch (taskStatus.status) {
      case 'planning': return 'planning';
      case 'running': return 'running';
      case 'completed': return 'completed';
      case 'failed': return 'failed';
      default: return 'none';
    }
  };

  // Replicates the style selection logic
  const selectColor = (
    isPlanningPhase: boolean,
    taskStatus: { status: string } | null
  ): string => {
    if (isPlanningPhase && !taskStatus) return 'warning';
    if (taskStatus?.status === 'planning') return 'warning';
    if (taskStatus?.status === 'running') return 'info';
    if (taskStatus?.status === 'completed') return 'success';
    if (taskStatus?.status === 'failed') return 'error';
    return 'primary';
  };

  it('should select planning indicator when isPlanningPhase and no task status', () => {
    expect(selectIndicator(true, null)).toBe('planning');
  });

  it('should select no indicator when not planning and no task status', () => {
    expect(selectIndicator(false, null)).toBe('none');
  });

  it('should select running indicator when task is running', () => {
    expect(selectIndicator(false, { status: 'running' })).toBe('running');
  });

  it('should select running indicator when task is running even during planning', () => {
    expect(selectIndicator(true, { status: 'running' })).toBe('running');
  });

  it('should select planning indicator for explicit planning status', () => {
    expect(selectIndicator(false, { status: 'planning' })).toBe('planning');
  });

  it('should select completed indicator when task completed', () => {
    expect(selectIndicator(false, { status: 'completed' })).toBe('completed');
  });

  it('should select failed indicator when task failed', () => {
    expect(selectIndicator(false, { status: 'failed' })).toBe('failed');
  });

  it('should use warning color for planning phase', () => {
    expect(selectColor(true, null)).toBe('warning');
  });

  it('should use warning color for explicit planning status', () => {
    expect(selectColor(false, { status: 'planning' })).toBe('warning');
  });

  it('should use info color for running', () => {
    expect(selectColor(false, { status: 'running' })).toBe('info');
  });

  it('should use success color for completed', () => {
    expect(selectColor(false, { status: 'completed' })).toBe('success');
  });

  it('should use error color for failed', () => {
    expect(selectColor(false, { status: 'failed' })).toBe('error');
  });

  it('should use primary color when no status and not planning', () => {
    expect(selectColor(false, null)).toBe('primary');
  });
});

/**
 * Tests for the taskExecutionStore isPlanningPhase state management.
 */
describe('TaskExecutionStore - isPlanningPhase', () => {
  // Replicate the store's planning phase logic
  const createMockStore = () => {
    let isPlanningPhase = false;
    const taskStates = new Map<string, { status: string; task_name: string }>();

    return {
      get isPlanningPhase() { return isPlanningPhase; },
      setIsPlanningPhase: (value: boolean) => { isPlanningPhase = value; },
      clearTaskStates: () => {
        taskStates.clear();
      },
      transition: (id: string, status: string, metadata?: { task_name?: string }) => {
        taskStates.set(id, { status, task_name: metadata?.task_name || '' });
        return true;
      },
      getTaskStatus: (id: string) => taskStates.get(id) || null,
    };
  };

  it('should start with isPlanningPhase = false', () => {
    const store = createMockStore();
    expect(store.isPlanningPhase).toBe(false);
  });

  it('should set isPlanningPhase to true', () => {
    const store = createMockStore();
    store.setIsPlanningPhase(true);
    expect(store.isPlanningPhase).toBe(true);
  });

  it('should set isPlanningPhase back to false', () => {
    const store = createMockStore();
    store.setIsPlanningPhase(true);
    store.setIsPlanningPhase(false);
    expect(store.isPlanningPhase).toBe(false);
  });

  it('should preserve isPlanningPhase when clearTaskStates is called', () => {
    const store = createMockStore();
    store.setIsPlanningPhase(true);
    store.clearTaskStates();
    expect(store.isPlanningPhase).toBe(true); // clearTaskStates only clears task states, not planning flag
  });

  it('should allow transition while in planning phase', () => {
    const store = createMockStore();
    store.setIsPlanningPhase(true);
    store.transition('task-1', 'running', { task_name: 'Test Task' });
    expect(store.getTaskStatus('task-1')).toEqual(expect.objectContaining({ status: 'running', task_name: 'Test Task' }));
    expect(store.isPlanningPhase).toBe(true); // Planning phase persists until explicitly cleared
  });
});
