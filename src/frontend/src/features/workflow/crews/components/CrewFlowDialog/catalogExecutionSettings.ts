import type { CrewResponse } from '../../../../../types/workflow/crew';
import type { CanvasExecutionConfig } from '../../../../../app/sessions/builderCanvasStore';

/** A catalog load replaces the previous crew's settings, including effort. */
export function catalogExecutionSettings(crew: CrewResponse, managerModel?: string, hasManager = false): CanvasExecutionConfig {
  const process = hasManager ? 'hierarchical' : crew.process;
  return {
    processType: process === 'hierarchical' || process === 'parallel' ? process : 'sequential',
    reasoningEnabled: crew.reasoning ?? false,
    reasoningLLM: crew.reasoning_llm ?? '',
    reasoningConfig: {
      ...crew.reasoning_config,
      execution_effort: crew.reasoning_config?.execution_effort,
    },
    managerLLM: crew.manager_llm || managerModel || '',
  };
}
