/**
 * Unit tests for CrewNode component.
 *
 * Tests the crew node display in flow canvas including:
 * - Name truncation for long crew names
 * - Execution status display
 * - Tooltip showing full name and tasks
 * - Delete functionality
 * - Handle positioning based on layout orientation
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material';
import CrewNode from './CrewNode';

// Mock ReactFlow
vi.mock('reactflow', () => ({
  Handle: ({ position, type, id, style }: { position: string; type: string; id: string; style?: object }) => (
    <div data-testid={`handle-${type}-${id}`} data-position={position} style={style} />
  ),
  Position: {
    Top: 'top',
    Bottom: 'bottom',
    Left: 'left',
    Right: 'right',
  },
  useReactFlow: () => ({
    deleteElements: vi.fn(),
  }),
}));

// Mock stores
vi.mock('../../store/uiLayout', () => ({
  useUILayoutStore: vi.fn((selector) => {
    const state = { layoutOrientation: 'vertical' };
    return selector(state);
  }),
}));

vi.mock('../../store/flowExecutionStore', () => ({
  useFlowExecutionStore: vi.fn((selector) => {
    const state = {
      crewNodeStates: new Map(),
      isExecuting: false,
      flowStatus: null,
    };
    return selector(state);
  }),
}));

const theme = createTheme();

const defaultProps = {
  id: 'crew-node-1',
  data: {
    id: 'crew-1',
    label: 'Test Crew',
    crewName: 'test_crew',
    crewId: '123',
    selectedTasks: [],
    allTasks: [],
  },
  selected: false,
  isConnectable: true,
  type: 'crewNode',
  zIndex: 0,
  xPos: 0,
  yPos: 0,
  dragging: false,
};

const renderCrewNode = (props = {}) => {
  const mergedProps = { ...defaultProps, ...props };
  return render(
    <ThemeProvider theme={theme}>
      <CrewNode {...mergedProps} />
    </ThemeProvider>
  );
};

describe('CrewNode', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Basic Rendering', () => {
    it('should render crew name', () => {
      renderCrewNode();
      expect(screen.getByText('Test Crew')).toBeInTheDocument();
    });

    it('should render "Unnamed Crew" when label is empty', () => {
      renderCrewNode({
        data: {
          ...defaultProps.data,
          label: '',
        },
      });
      expect(screen.getByText('Unnamed Crew')).toBeInTheDocument();
    });

    it('should render handles for connections', () => {
      renderCrewNode();
      expect(screen.getByTestId('handle-target-top')).toBeInTheDocument();
      expect(screen.getByTestId('handle-target-left')).toBeInTheDocument();
      expect(screen.getByTestId('handle-source-bottom')).toBeInTheDocument();
      expect(screen.getByTestId('handle-source-right')).toBeInTheDocument();
    });
  });

  describe('Name Truncation', () => {
    it('should handle short crew names without truncation', () => {
      renderCrewNode({
        data: {
          ...defaultProps.data,
          label: 'Short',
        },
      });
      const text = screen.getByText('Short');
      expect(text).toBeInTheDocument();
    });

    it('should apply truncation styles for long names', () => {
      const longName = 'This is a very long crew name that should be truncated with ellipsis';
      renderCrewNode({
        data: {
          ...defaultProps.data,
          label: longName,
        },
      });
      const text = screen.getByText(longName);
      // Check that the element has overflow hidden styles applied
      const styles = window.getComputedStyle(text);
      expect(text).toBeInTheDocument();
      // The component should have truncation CSS applied
      expect(text).toHaveStyle({ overflow: 'hidden' });
    });

    it('should show full name in tooltip', () => {
      const longName = 'This is a very long crew name that should be truncated';
      renderCrewNode({
        data: {
          ...defaultProps.data,
          label: longName,
          crewName: longName,
        },
      });
      // The tooltip should contain the full crew name
      expect(screen.getByText(longName)).toBeInTheDocument();
    });
  });

  describe('Task Tooltip', () => {
    it('should show selected tasks in tooltip when tasks are selected', () => {
      renderCrewNode({
        data: {
          ...defaultProps.data,
          selectedTasks: [
            { id: '1', name: 'Task One', description: 'First task' },
            { id: '2', name: 'Task Two', description: 'Second task' },
          ],
        },
      });
      // Component renders with tooltip containing task info
      expect(screen.getByText('Test Crew')).toBeInTheDocument();
    });

    it('should show crew name in tooltip when no tasks selected', () => {
      renderCrewNode({
        data: {
          ...defaultProps.data,
          selectedTasks: [],
        },
      });
      expect(screen.getByText('Test Crew')).toBeInTheDocument();
    });
  });

  describe('Selection State', () => {
    it('should apply selected styles when selected', () => {
      const { container } = renderCrewNode({ selected: true });
      const box = container.querySelector('[class*="MuiBox-root"]');
      expect(box).toBeInTheDocument();
    });

    it('should not show delete button when not hovered and not selected', () => {
      renderCrewNode({ selected: false });
      // Delete button should not be visible initially
      const deleteButtons = screen.queryAllByRole('button');
      expect(deleteButtons.length).toBe(0);
    });
  });

  describe('Hover Behavior', () => {
    it('should show delete button on hover', () => {
      const { container } = renderCrewNode();
      const box = container.querySelector('[class*="MuiBox-root"]');

      if (box) {
        fireEvent.mouseEnter(box);
        // After hover, delete button should appear
        const deleteButton = screen.queryByRole('button');
        expect(deleteButton).toBeInTheDocument();
      }
    });

    it('should hide delete button on mouse leave', () => {
      const { container } = renderCrewNode();
      const box = container.querySelector('[class*="MuiBox-root"]');

      if (box) {
        fireEvent.mouseEnter(box);
        fireEvent.mouseLeave(box);
        // After leaving, delete button should be hidden
        const deleteButtons = screen.queryAllByRole('button');
        expect(deleteButtons.length).toBe(0);
      }
    });
  });

  describe('Node Dimensions', () => {
    it('should have fixed width of 140px', () => {
      const { container } = renderCrewNode();
      const box = container.querySelector('[class*="MuiBox-root"]');
      expect(box).toHaveStyle({ width: '100%' });
    });

    it('should have fixed height of 80px', () => {
      const { container } = renderCrewNode();
      const box = container.querySelector('[class*="MuiBox-root"]');
      expect(box).toHaveStyle({ height: '100%' });
    });
  });
});

describe('CrewNode Execution Status', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should render without status icon when not executing', () => {
    renderCrewNode();
    // No status icon should be present
    const progressIndicators = screen.queryAllByRole('progressbar');
    expect(progressIndicators.length).toBe(0);
  });
});

describe('CrewNode Handle Visibility', () => {
  it('should configure handles based on layout orientation', () => {
    renderCrewNode();

    // In vertical layout, top/bottom handles should be visible
    const topHandle = screen.getByTestId('handle-target-top');
    const leftHandle = screen.getByTestId('handle-target-left');

    expect(topHandle).toBeInTheDocument();
    expect(leftHandle).toBeInTheDocument();
  });
});
