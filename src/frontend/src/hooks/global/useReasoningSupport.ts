import { useMemo } from 'react';
import { useTabManagerStore } from '../../store/tabManager';

/**
 * Whether the reasoning control can do anything for the crew on the canvas.
 *
 * Reasoning is the model's OWN native thinking budget, applied per agent — so
 * whether the setting has any effect depends on the agents currently on the
 * canvas, not on a crew-level model. The backend drops `reasoning_effort` for a
 * model without a budget and says so at INFO ("has no native reasoning budget —
 * reasoning_effort='high' has no effect"), a log written precisely because the
 * setting otherwise LOOKED applied and the run looked identical.
 *
 * Shared rather than duplicated: the control now appears in the composer's "+"
 * menu, and a second copy of this rule is how the two would quietly disagree
 * about whether reasoning is available.
 */
export interface ReasoningSupport {
  /** Distinct models across the agents on the canvas — names the helper text. */
  agentModelNames: string[];
  /** True when ANY agent's model has a reasoning budget. */
  supported: boolean;
}

/** Minimal shape needed from the model catalogue. */
export type ReasoningModelCatalogue = Record<
  string,
  { supports_reasoning_effort?: boolean } | undefined
>;

export function useReasoningSupport(
  models: ReasoningModelCatalogue,
  selectedModel?: string,
): ReasoningSupport {
  // Canvas nodes come from the ACTIVE TAB, not useWorkflowStore: that store has
  // a single shared nodes array that goes stale when switching between the crew
  // and flow canvases.
  const activeTabNodes = useTabManagerStore(
    (s) => s.tabs.find((t) => t.id === s.activeTabId)?.nodes,
  );

  // The models the NEXT run could actually use — both halves matter.
  //
  // The agents already on the canvas keep whatever llm they were created with.
  // But the composer's selected model is what generation stamps on the agents it
  // is about to create (WorkflowChatRefactored dispatches with `model:
  // selectedModel`), so leaving it out produced the reported bug exactly: pick a
  // GPT model in the composer, and the menu still reported the canvas's stale
  // Qwen agent as the reason reasoning was unavailable — naming a model the user
  // had just replaced.
  const agentModelNames = useMemo(() => {
    const seen = new Set<string>();
    if (selectedModel) seen.add(selectedModel);
    for (const node of activeTabNodes || []) {
      if (node.type !== 'agentNode') continue;
      const llm = (node.data as { llm?: string } | undefined)?.llm;
      if (llm) seen.add(llm);
    }
    return Array.from(seen);
  }, [activeTabNodes, selectedModel]);

  // Enabled when ANY candidate has a budget: a mixed crew still benefits, and
  // the engine applies the effort per agent.
  const supported = useMemo(
    () => agentModelNames.some((key) => models[key]?.supports_reasoning_effort),
    [agentModelNames, models],
  );

  return { agentModelNames, supported };
}

/** The helper text shown when the setting would be ignored. */
export function reasoningUnsupportedReason(agentModelNames: string[]): string {
  return agentModelNames.length
    ? `${agentModelNames.join(', ')} has no reasoning budget — this setting would be ignored.`
    : 'Add an agent whose model has a reasoning budget (e.g. a GPT-5 / o3 / o4 model).';
}
