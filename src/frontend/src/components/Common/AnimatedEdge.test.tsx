import { render, fireEvent, screen } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Position, ReactFlowProvider } from 'reactflow';
import AnimatedEdge from './AnimatedEdge';

// Control the run-status store per test so we can assert the animation gating.
let hasRunningJobs = false;
vi.mock('../../store/runStatus', () => ({
  useRunStatusStore: (selector: (s: { hasRunningJobs: boolean }) => unknown) =>
    selector({ hasRunningJobs }),
}));

// Stub the heavy flow state form so the edge dialog can mount in isolation.
vi.mock('../Flow', () => ({
  EdgeStateForm: ({ onSubmit, onCancel }: { onSubmit: (d: unknown) => void; onCancel: () => void }) => (
    <div data-testid="edge-state-form">
      <button onClick={() => onSubmit({ stateType: 'structured', stateDefinition: 'x' })}>submit</button>
      <button onClick={onCancel}>cancel</button>
    </div>
  ),
}));

// jsdom doesn't implement SVG getBBox, which ReactFlow's EdgeText calls on mount.
beforeEach(() => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (SVGElement.prototype as any).getBBox = () => ({ x: 0, y: 0, width: 40, height: 16 });
});

const baseProps = {
  id: 'edge-1',
  source: 'task-a',
  target: 'task-b',
  sourceX: 0,
  sourceY: 0,
  targetX: 100,
  targetY: 100,
  sourcePosition: Position.Right,
  targetPosition: Position.Left,
  markerEnd: '',
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} as any;

const renderEdge = (props: Record<string, unknown> = {}) =>
  render(
    <ReactFlowProvider>
      <svg>
        <AnimatedEdge {...baseProps} {...props} />
      </svg>
    </ReactFlowProvider>
  );

const getPath = (container: HTMLElement) =>
  container.querySelector('path.react-flow__edge-path') as SVGPathElement;

describe('AnimatedEdge', () => {
  beforeEach(() => {
    hasRunningJobs = false;
    vi.restoreAllMocks();
  });

  it('renders the edge path', () => {
    const { container } = renderEdge();
    expect(getPath(container)).toBeTruthy();
  });

  describe('animation gating on run status', () => {
    it('forces animation: none when no job is running (even for an animated edge)', () => {
      hasRunningJobs = false;
      const { container } = renderEdge({ animated: true });
      expect(getPath(container).style.animation).toBe('none');
    });

    it('does not force animation off while a job is running (lets ReactFlow animate)', () => {
      hasRunningJobs = true;
      const { container } = renderEdge({ animated: true });
      // When running we leave the inline animation unset (undefined) so
      // ReactFlow's built-in `.animated` class drives the flow.
      expect(getPath(container).style.animation).toBe('');
    });

    it('keeps animation: none for a non-animated edge while running', () => {
      hasRunningJobs = true;
      const { container } = renderEdge({ animated: false });
      expect(getPath(container).style.animation).toBe('none');
    });
  });

  describe('hover affordances', () => {
    it('shows the label and delete control on hover and dispatches edge:delete on click', () => {
      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      const { container } = renderEdge();
      const hitArea = container.querySelector('rect') as SVGRectElement;

      fireEvent.mouseEnter(hitArea);
      // dependency label for a task-to-task edge
      expect(screen.getByText('dependency')).toBeInTheDocument();

      const deleteGroup = container.querySelector('g[transform][style*="cursor"]') as SVGGElement;
      expect(deleteGroup).toBeTruthy();
      fireEvent.click(deleteGroup);

      expect(
        dispatchSpy.mock.calls.some(([e]) => (e as CustomEvent).type === 'edge:delete')
      ).toBe(true);
    });

    it('hides the hover affordances when the pointer leaves the edge area', () => {
      const { container } = renderEdge();
      const hitArea = container.querySelector('rect') as SVGRectElement;

      fireEvent.mouseEnter(hitArea);
      expect(screen.getByText('dependency')).toBeInTheDocument();

      // Coords well outside the (0-sized in jsdom) bounds count as leaving.
      fireEvent.mouseLeave(hitArea, { clientX: 9999, clientY: 9999 });
      expect(screen.queryByText('dependency')).not.toBeInTheDocument();
    });
  });

  describe('flow edges', () => {
    const flowProps = { source: 'flow-1', target: 'flow-2' };

    it('opens the state dialog and dispatches edge:update on submit', () => {
      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      const { container } = renderEdge(flowProps);
      const hitArea = container.querySelector('rect') as SVGRectElement;
      fireEvent.mouseEnter(hitArea);

      // settings button (only present for flow edges) is the cursor group with a circle
      const groups = Array.from(
        container.querySelectorAll('g[transform][style*="cursor"]')
      ) as SVGGElement[];
      const settingsGroup = groups.find((g) => g.querySelector('circle')) as SVGGElement;
      expect(settingsGroup).toBeTruthy();
      fireEvent.click(settingsGroup);

      expect(screen.getByTestId('edge-state-form')).toBeInTheDocument();
      fireEvent.click(screen.getByText('submit'));

      expect(
        dispatchSpy.mock.calls.some(([e]) => (e as CustomEvent).type === 'edge:update')
      ).toBe(true);
    });

    it('closes the state dialog on cancel without dispatching an update', () => {
      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      const { container } = renderEdge(flowProps);
      fireEvent.mouseEnter(container.querySelector('rect') as SVGRectElement);

      const groups = Array.from(
        container.querySelectorAll('g[transform][style*="cursor"]')
      ) as SVGGElement[];
      const settingsGroup = groups.find((g) => g.querySelector('circle')) as SVGGElement;
      fireEvent.click(settingsGroup);
      expect(screen.getByTestId('edge-state-form')).toBeInTheDocument();

      fireEvent.click(screen.getByText('cancel'));
      // Cancelling must not emit an update event.
      expect(
        dispatchSpy.mock.calls.some(([e]) => (e as CustomEvent).type === 'edge:update')
      ).toBe(false);
    });
  });
});
