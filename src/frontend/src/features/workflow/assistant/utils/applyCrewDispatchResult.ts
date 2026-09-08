import type React from 'react';
import type { Node } from 'reactflow';
import type { DispatchResult, StreamingGenerationResult, ConfigureCrewResult, CatalogLoadResult, FlowLoadResult } from '../../../../api/execution/DispatcherService';
import type { ChatMessage, GeneratedCrew, GeneratedAgent, GeneratedTask } from '../types';
import { handleConfigureCrew } from './nodeGenerationHandlers';
import { hasCrewContent } from './chatHelpers';
import { getAssistantResponse } from './assistantResponse';

interface Options {
  jobId?: string;
  generationCompletedRef: React.MutableRefObject<boolean>;
  detachTabFromSavedCrew: () => void;
  handleCrewGenerated: (crew: GeneratedCrew) => void;
  handleAgentGenerated: (agent: GeneratedAgent) => Promise<void>;
  handleTaskGenerated: (task: GeneratedTask) => Promise<void>;
  setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>;
  saveMessageToBackend: (message: ChatMessage) => Promise<void>;
  setGenerationId: (id: string | null) => void;
  inputRef: React.RefObject<HTMLInputElement>;
  onExecuteCrew?: () => void;
  nodes: Node[];
}
export async function applyCrewDispatchResult(result: DispatchResult, options: Options) {
  const { generationCompletedRef, detachTabFromSavedCrew, handleCrewGenerated, handleAgentGenerated, handleTaskGenerated,
    setMessages, saveMessageToBackend, setGenerationId, inputRef, onExecuteCrew, nodes, jobId } = options;
  const completedGeneration = (result.generation_result as StreamingGenerationResult | null)?.completed;
  if (completedGeneration) {
    if (!generationCompletedRef.current) {
      // Recover the complete canvas if a proxy dropped progressive SSE.
      const crew = (result.generation_result as StreamingGenerationResult).generated_crew;
      if (crew?.agents?.length && crew?.tasks?.length) {
        detachTabFromSavedCrew();
        handleCrewGenerated(crew);
        const recovered: ChatMessage = { id: jobId ? `msg-progress-${jobId}` : `generated-${Date.now()}`, type: 'assistant', jobId, timestamp: new Date(), content: getAssistantResponse({ ...result, dispatcher: { ...result.dispatcher, intent: 'generate_crew' }, generation_result: crew }) + '\n\n✓ Crew generated successfully', metadata: { catalogKind: 'crew', catalogName: crew.tasks[0]?.name || crew.agents[0]?.name } };
        setMessages(prev => [...prev.filter(message => message.id !== recovered.id), recovered]);
        await saveMessageToBackend(recovered);
      }
    }
    return;
  }

  const assistantMessage: ChatMessage = {
    id: `msg-${Date.now() + 1}`,
    type: 'assistant',
    content: getAssistantResponse(result),
    timestamp: new Date(),
    intent: result.dispatcher.intent,
    confidence: result.dispatcher.confidence,
    result: result.generation_result,
  };

  setMessages(prev => [...prev, assistantMessage]);
  saveMessageToBackend(assistantMessage);

  if (result.generation_result) {
    switch (result.dispatcher.intent) {
      case 'generate_agent':
        await handleAgentGenerated(result.generation_result as GeneratedAgent);
        break;
      case 'generate_task':
        await handleTaskGenerated(result.generation_result as GeneratedTask);
        break;
      case 'generate_crew':
      case 'generate_plan': {
        const genResult = result.generation_result as StreamingGenerationResult | GeneratedCrew;
        if (genResult && typeof genResult === 'object' && 'type' in genResult && genResult.type === 'streaming') {
          // Progressive SSE path
          const streamResult = genResult as StreamingGenerationResult;
          setGenerationId(streamResult.generation_id);
          // Keep isLoading true — it will be cleared by onComplete/onFailed
        } else {
          // Legacy synchronous path (fallback)
          detachTabFromSavedCrew();
          handleCrewGenerated(genResult as GeneratedCrew);
          const crew = genResult as GeneratedCrew;
          if (crew.agents?.length && crew.tasks?.length) {
            setMessages(prev => prev.map(message => message.id === assistantMessage.id
              ? { ...message, metadata: { ...message.metadata, catalogKind: 'crew', catalogName: crew.tasks?.[0]?.name || crew.agents?.[0]?.name } }
              : message));
          }
        }
        break;
      }
      case 'configure_crew':
        handleConfigureCrew(result.generation_result as ConfigureCrewResult, inputRef);
        break;
      case 'catalog_list':
      case 'catalog_help':
        // Handled via response message only (no canvas action)
        break;
      case 'catalog_load': {
        const loadResult = result.generation_result as CatalogLoadResult;
        if (loadResult.plan?.nodes) {
          // Dispatch custom event for WorkflowDesigner to handle via handleCrewSelectWrapper
          const loadEvent = new CustomEvent('catalogLoadCrew', {
            detail: {
              nodes: loadResult.plan.nodes,
              edges: loadResult.plan.edges,
              name: loadResult.plan.name,
              id: loadResult.plan.id,
            },
          });
          window.dispatchEvent(loadEvent);
        }
        break;
      }
      case 'catalog_save': {
        const saveResult = result.generation_result as { suggested_name?: string; message: string };
        const saveEvent = new CustomEvent('openSaveCrewDialog', {
          detail: { suggestedName: saveResult.suggested_name },
        });
        window.dispatchEvent(saveEvent);
        break;
      }
      case 'catalog_schedule': {
        const scheduleEvent = new CustomEvent('openScheduleDialog');
        window.dispatchEvent(scheduleEvent);
        break;
      }
      case 'flow_list':
      case 'catalog_delete':
      case 'flow_delete':
        // Handled via response message only (no canvas action)
        break;
      case 'flow_load': {
        const flowLoadResult = result.generation_result as FlowLoadResult;
        if (flowLoadResult.flow?.nodes) {
          window.dispatchEvent(new CustomEvent('catalogLoadFlow', {
            detail: {
              nodes: flowLoadResult.flow.nodes,
              edges: flowLoadResult.flow.edges,
              flowConfig: flowLoadResult.flow.flow_config,
              name: flowLoadResult.flow.name,
              id: flowLoadResult.flow.id,
            },
          }));
        }
        break;
      }
      case 'flow_save': {
        const flowSaveResult = result.generation_result as { suggested_name?: string; message: string };
        window.dispatchEvent(new CustomEvent('openSaveFlowDialog', {
          detail: { suggestedName: flowSaveResult.suggested_name },
        }));
        break;
      }
      case 'execute_crew': {
        const execResult = result.generation_result as { plan?: CatalogLoadResult['plan']; message: string };
        if (execResult.plan?.nodes) {
          // Load crew on canvas first
          window.dispatchEvent(new CustomEvent('catalogLoadCrew', {
            detail: {
              nodes: execResult.plan.nodes,
              edges: execResult.plan.edges,
              name: execResult.plan.name,
              id: execResult.plan.id,
            },
          }));
          // Give canvas time to render, then trigger execution
          setTimeout(() => {
            if (onExecuteCrew) {
              onExecuteCrew();
            }
          }, 500);
        } else if (!execResult.plan) {
          // No name provided — execute whatever is on canvas
          if (onExecuteCrew && hasCrewContent(nodes)) {
            onExecuteCrew();
          }
        }
        break;
      }
      case 'execute_flow': {
        const execFlowResult = result.generation_result as { flow?: FlowLoadResult['flow']; message: string };
        if (execFlowResult.flow?.nodes) {
          // Load flow on canvas first
          window.dispatchEvent(new CustomEvent('catalogLoadFlow', {
            detail: {
              nodes: execFlowResult.flow.nodes,
              edges: execFlowResult.flow.edges,
              flowConfig: execFlowResult.flow.flow_config,
              name: execFlowResult.flow.name,
              id: execFlowResult.flow.id,
            },
          }));
          // Give canvas time to render, then trigger flow execution
          setTimeout(() => {
            window.dispatchEvent(new CustomEvent('executeFlowEvent'));
          }, 500);
        } else if (!execFlowResult.flow) {
          // No name provided — execute whatever is on canvas
          window.dispatchEvent(new CustomEvent('executeFlowEvent'));
        }
        break;
      }
    }
  }

}
