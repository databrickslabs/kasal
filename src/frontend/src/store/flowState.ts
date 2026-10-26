/**
 * The part of a flow's state declaration that cannot be derived.
 *
 * Channel NAMES come from the canvas — `deriveFlowStateConfig` reads them out
 * of router conditions and `{placeholders}`, and rederives them on every save,
 * update and run so the three cannot drift. Reducers and the conversational
 * flag are different: nothing in a condition says whether a channel accumulates
 * or whether the flow holds a conversation. Those are decisions, and a decision
 * has to be stored.
 *
 * Kept per TAB rather than globally, because two tabs can hold two flows.
 * Persisted, because losing an unsaved declaration to a page reload would be
 * indistinguishable from the feature not working.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { FlowStateConfig, FlowStateReducer } from '../utils/flowStateSchema';

export type DeclaredFlowState = Pick<
  FlowStateConfig,
  'conversational' | 'initialValues'
> & {
  model?: { type: 'object'; properties: Record<string, { reducer?: FlowStateReducer }> };
};

interface FlowStateStore {
  /** Keyed by tab id. */
  declared: Record<string, DeclaredFlowState>;

  getDeclared: (tabId?: string | null) => DeclaredFlowState | undefined;
  setDeclared: (tabId: string, declared: DeclaredFlowState) => void;
  /** Replace one channel's reducer, leaving everything else alone. */
  setReducer: (tabId: string, channel: string, reducer: FlowStateReducer) => void;
  setConversational: (tabId: string, conversational: boolean) => void;
  clearDeclared: (tabId: string) => void;
}

export const useFlowStateStore = create<FlowStateStore>()(
  persist(
    (set, get) => ({
      declared: {},

      getDeclared: (tabId) => (tabId ? get().declared[tabId] : undefined),

      setDeclared: (tabId, declared) =>
        set((state) => ({ declared: { ...state.declared, [tabId]: declared } })),

      setReducer: (tabId, channel, reducer) =>
        set((state) => {
          const current = state.declared[tabId] ?? {};
          const properties = { ...(current.model?.properties ?? {}) };
          if (reducer === 'replace') {
            // `replace` is the default everywhere. Storing it would still be
            // correct, but an empty declaration reads as "nothing decided
            // here", which is what a channel left alone actually means.
            delete properties[channel];
          } else {
            properties[channel] = { reducer };
          }
          return {
            declared: {
              ...state.declared,
              [tabId]: { ...current, model: { type: 'object', properties } },
            },
          };
        }),

      setConversational: (tabId, conversational) =>
        set((state) => ({
          declared: {
            ...state.declared,
            [tabId]: { ...(state.declared[tabId] ?? {}), conversational },
          },
        })),

      clearDeclared: (tabId) =>
        set((state) => {
          const declared = { ...state.declared };
          delete declared[tabId];
          return { declared };
        }),
    }),
    { name: 'flow-state-declaration' },
  ),
);

/**
 * The declaration for a tab, as `buildFlowConfiguration` wants it.
 *
 * A helper rather than a raw store read because every caller needs the same
 * thing — the current tab's declaration — and the ones that need it are save
 * paths, where reaching for the wrong tab silently writes one flow's
 * declaration onto another.
 */
export function declaredStateForTab(
  tabId?: string | null,
): Partial<FlowStateConfig> | undefined {
  return useFlowStateStore.getState().getDeclared(tabId) as
    | Partial<FlowStateConfig>
    | undefined;
}
