/**
 * handleCrewSelectWrapper — loading a crew into a new tab.
 *
 * Loading a crew must always land on the crew canvas, even if the user was on the
 * flow canvas: the new tab is created with an explicit 'crew' view mode.
 */
import { renderHook, act } from '@testing-library/react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Node, Edge } from 'reactflow';
import { useEventBindings } from './WorkflowEventHandlers';
import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore } from '../../store/uiLayout';

const crewNode = (id: string): Node => ({
  id,
  type: 'crewNode',
  position: { x: 0, y: 0 },
  data: { label: id },
});

describe('handleCrewSelectWrapper forces the crew canvas', () => {
  beforeEach(() => {
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  it('creates the loaded-crew tab in crew view even when on the flow canvas', () => {
    useUILayoutStore.setState({ areFlowsVisible: true });

    const { result } = renderHook(() =>
      useEventBindings(async () => {}, vi.fn(), vi.fn()),
    );

    act(() => {
      result.current.handleCrewSelectWrapper(
        [crewNode('a')],
        [] as Edge[],
        'My Crew',
        'crew-123',
      );
    });

    const active = useTabManagerStore.getState().getActiveTab();
    expect(active?.viewMode).toBe('crew');
    expect(active?.name).toBe('My Crew');
  });
});
