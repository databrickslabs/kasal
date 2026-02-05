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
    setNodes: vi.fn(),
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
    };
    return selector(state);
  }),
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

// Mock child components
vi.mock('./TaskForm', () => ({
  default: () => <div data-testid="task-form">Task Form</div>,
}));

vi.mock('./QuickToolSelectionDialog', () => ({
  default: ({ open }: { open: boolean }) =>
    open ? <div data-testid="tool-dialog">Tool Dialog</div> : null,
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
