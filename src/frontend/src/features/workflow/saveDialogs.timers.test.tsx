/**
 * SaveFlow / SaveCrew completion timers must not outlive the component.
 *
 * Regression for CI run 35632432981: SaveFlow dispatched `updateFlowComplete`
 * from a bare 100 ms setTimeout. When the test file's jsdom was torn down
 * first, the callback threw `ReferenceError: window is not defined` and Vitest
 * failed the run although every test had passed. Each case below starts a real
 * save through the component's window event, unmounts while the completion
 * timer is pending, and asserts that nothing is left pending and nothing fires.
 */
import React from 'react';
import { act, render } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SaveCrew from './crews/components/SaveCrew';
import SaveFlow from './flows/components/SaveFlow';
import { useBuilderCanvasStore } from '../../app/sessions/builderCanvasStore';

const api = vi.hoisted(() => ({
  saveCrew: vi.fn(),
  updateCrew: vi.fn(),
  getCrews: vi.fn(),
  saveFlow: vi.fn(),
  updateFlow: vi.fn(),
}));
vi.mock('../../api/workflow/CrewService', () => ({ CrewService: api }));
vi.mock('../../api/workflow/FlowService', () => ({ FlowService: api }));
vi.mock('../chat/store/appStore', () => ({ useAppStore: { getState: () => ({ loadCatalog: vi.fn() }) } }));

type Case = {
  label: string;
  flow: boolean;
  /** The window event that starts the save. */
  start: (tabId: string) => CustomEvent;
  /** The API call the save makes before it schedules the completion event. */
  call: () => ReturnType<typeof vi.fn>;
  /** The completion event the timer dispatches. */
  done: string;
};

const cases: Case[] = [
  {
    label: 'SaveFlow update of an existing flow',
    flow: true,
    start: tabId => new CustomEvent('updateExistingFlow', { detail: { tabId, flowId: 'existing' } }),
    call: () => api.updateFlow,
    done: 'updateFlowComplete',
  },
  {
    label: 'SaveFlow named save',
    flow: true,
    start: () => new CustomEvent('openSaveFlowDialog', { detail: { suggestedName: 'Swiss news' } }),
    call: () => api.saveFlow,
    done: 'saveFlowComplete',
  },
  {
    label: 'SaveCrew update of an existing crew',
    flow: false,
    start: tabId => new CustomEvent('updateExistingCrew', { detail: { tabId, crewId: 'existing' } }),
    call: () => api.updateCrew,
    done: 'updateCrewComplete',
  },
  {
    label: 'SaveCrew update by name',
    flow: false,
    start: tabId => new CustomEvent('updateExistingCrewByName', { detail: { tabId, crewName: 'Swiss news' } }),
    call: () => api.updateCrew,
    done: 'updateCrewComplete',
  },
  {
    label: 'SaveCrew named save',
    flow: false,
    start: () => new CustomEvent('openSaveCrewDialog', { detail: { suggestedName: 'Swiss news' } }),
    call: () => api.saveCrew,
    done: 'saveCrewComplete',
  },
];

/** Let the async save handler run to the point where it schedules its timer. */
async function settle(until: () => boolean): Promise<void> {
  for (let i = 0; i < 50 && !until(); i++) {
    await act(async () => {
      await Promise.resolve();
    });
  }
  expect(until()).toBe(true);
}

function mountSaving(c: Case) {
  const tabs = useBuilderCanvasStore.getState();
  const tabId = tabs.createCanvas('Swiss news', c.flow ? 'flow' : 'crew');
  const nodes = [{ id: 'node-1', type: c.flow ? 'crewNode' : 'agentNode', position: { x: 0, y: 0 }, data: { crewId: 'crew-1' } }];
  if (c.flow) tabs.updateCanvasFlowNodes(tabId, nodes);
  else tabs.updateCanvasNodes(tabId, nodes);
  const Component = c.flow ? SaveFlow : SaveCrew;
  return render(<Component nodes={nodes} edges={[]} trigger={<button>save</button>} />);
}

describe.each(cases)('$label', c => {
  const completed = vi.fn();

  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
    [api.saveCrew, api.updateCrew, api.saveFlow, api.updateFlow].forEach(fn =>
      fn.mockResolvedValue({ id: 'saved-id', name: 'Swiss news' }),
    );
    api.getCrews.mockResolvedValue([{ id: 'existing', name: 'Swiss news' }]);
    window.addEventListener(c.done, completed);
  });

  afterEach(() => {
    window.removeEventListener(c.done, completed);
    vi.useRealTimers();
  });

  it('dispatches the completion event after 100 ms while mounted', async () => {
    const view = mountSaving(c);
    const tabId = useBuilderCanvasStore.getState().canvases[0].id;
    act(() => {
      window.dispatchEvent(c.start(tabId));
    });
    await settle(() => c.call().mock.calls.length > 0 && vi.getTimerCount() > 0);

    expect(completed).not.toHaveBeenCalled();
    act(() => {
      vi.advanceTimersByTime(100);
    });
    expect(completed).toHaveBeenCalledTimes(1);
    view.unmount();
  });

  it('cancels the pending completion timer when unmounted mid-timer', async () => {
    const view = mountSaving(c);
    const tabId = useBuilderCanvasStore.getState().canvases[0].id;
    act(() => {
      window.dispatchEvent(c.start(tabId));
    });
    await settle(() => c.call().mock.calls.length > 0 && vi.getTimerCount() > 0);

    act(() => {
      vi.advanceTimersByTime(50);
    });
    view.unmount();

    expect(vi.getTimerCount()).toBe(0);
    vi.runAllTimers();
    expect(completed).not.toHaveBeenCalled();
  });
});
