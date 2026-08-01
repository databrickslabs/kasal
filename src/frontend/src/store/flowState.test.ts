/**
 * The declared half of a flow's state — the part derivation cannot recover.
 *
 * Channel names are rederived from the canvas on every save, update and run.
 * Reducers and the conversational flag are decisions, so they are stored, and
 * the thing that must never happen is a rebuild quietly dropping them: an
 * appending channel demoted to overwriting makes a conversational flow forget
 * everything but its newest turn, and the run still reports success.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { useFlowStateStore, declaredStateForTab } from './flowState';
import { deriveFlowStateConfig } from '../utils/flowStateSchema';

const TAB = 'tab-1';

const crewNode = {
  id: 'crew-1',
  type: 'crewNode',
  data: {
    allTasks: [{ id: 't1', name: 'search', description: 'find {topic} news' }],
  },
};
const routerEdge = {
  id: 'e1',
  source: 'crew-1',
  target: 'crew-2',
  data: { logicType: 'ROUTER', routerCondition: 'state.get("has_results", "") == True' },
};

beforeEach(() => {
  useFlowStateStore.setState({ declared: {} });
});

describe('the declaration store', () => {
  it('keeps a reducer per channel', () => {
    useFlowStateStore.getState().setReducer(TAB, 'findings', 'append');

    expect(declaredStateForTab(TAB)?.model?.properties.findings).toEqual({
      reducer: 'append',
    });
  });

  it('drops a channel set back to the default', () => {
    // `replace` is what an undeclared channel already does, so storing it would
    // record a decision where none was made.
    useFlowStateStore.getState().setReducer(TAB, 'findings', 'append');
    useFlowStateStore.getState().setReducer(TAB, 'findings', 'replace');

    expect(declaredStateForTab(TAB)?.model?.properties.findings).toBeUndefined();
  });

  it('keeps the conversational flag beside the channels', () => {
    useFlowStateStore.getState().setReducer(TAB, 'findings', 'append');
    useFlowStateStore.getState().setConversational(TAB, true);

    const declared = declaredStateForTab(TAB);
    expect(declared?.conversational).toBe(true);
    expect(declared?.model?.properties.findings).toEqual({ reducer: 'append' });
  });

  it('keeps two tabs apart', () => {
    // Two tabs can hold two flows; writing one tab's declaration onto the other
    // would change a flow nobody was editing.
    useFlowStateStore.getState().setConversational('tab-a', true);
    useFlowStateStore.getState().setReducer('tab-b', 'notes', 'append');

    expect(declaredStateForTab('tab-a')?.conversational).toBe(true);
    expect(declaredStateForTab('tab-b')?.conversational).toBeUndefined();
  });

  it('clears a tab so the next flow does not inherit it', () => {
    useFlowStateStore.getState().setConversational(TAB, true);
    useFlowStateStore.getState().clearDeclared(TAB);

    expect(declaredStateForTab(TAB)).toBeUndefined();
  });

  it('has nothing for an unknown tab', () => {
    expect(declaredStateForTab(undefined)).toBeUndefined();
    expect(declaredStateForTab('never-opened')).toBeUndefined();
  });
});

describe('the declaration survives a rebuild', () => {
  it('a declared reducer reaches the emitted config', () => {
    useFlowStateStore.getState().setReducer(TAB, 'has_results', 'append');

    const config = deriveFlowStateConfig(
      [crewNode],
      [routerEdge],
      declaredStateForTab(TAB),
    );

    expect(config?.model.properties.has_results).toEqual({ reducer: 'append' });
    // Derived names still come from the canvas.
    expect(Object.keys(config!.model.properties).sort()).toEqual([
      'has_results',
      'topic',
    ]);
  });

  it('a conversational flow with no readable channels still declares state', () => {
    // The turn contract has to live somewhere, so `conversational` alone is
    // enough to stop the flow running on an untyped dict.
    useFlowStateStore.getState().setConversational(TAB, true);

    const config = deriveFlowStateConfig([], [], declaredStateForTab(TAB));

    expect(config?.conversational).toBe(true);
    expect(config?.enabled).toBe(true);
  });

  it('a declared channel the canvas no longer mentions is kept', () => {
    // Deleting the router that read it should not silently drop the author's
    // decision about it — and a stale channel costs nothing, while a missing
    // one rejects a legitimate input.
    useFlowStateStore.getState().setReducer(TAB, 'retired', 'append');

    const config = deriveFlowStateConfig([crewNode], [], declaredStateForTab(TAB));

    expect(config?.model.properties.retired).toEqual({ reducer: 'append' });
  });

  it('nothing declared and nothing derived stays undefined', () => {
    expect(deriveFlowStateConfig([], [], declaredStateForTab(TAB))).toBeUndefined();
  });
});
