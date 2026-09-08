import type { Dispatch, SetStateAction, MutableRefObject } from 'react';
import type { Node } from 'reactflow';
import { FlowService } from '../../../../api/workflow/FlowService';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { useTabManagerStore } from '../../../../store/tabManager';
import type { ChatMessage, FlowDraft } from '../types';

interface FlowTurnOptions {
  inputValue: string;
  selectedModel: string;
  nodes: Node[];
  flowRequest: MutableRefObject<AbortController | null>;
  setMessages: Dispatch<SetStateAction<ChatMessage[]>>;
  setInputValue: (value: string) => void;
  setIsLoading: (value: boolean) => void;
  saveMessageToBackend: (message: ChatMessage) => Promise<void>;
  onFlowGenerated?: (draft: FlowDraft) => void;
  beginGenerationTrace: (jobId: string) => void;
  setGenerationTraceId: (jobId: string | null) => void;
}

export async function generateFlowTurn({ inputValue, selectedModel, nodes, flowRequest, setMessages, setInputValue, setIsLoading, saveMessageToBackend, onFlowGenerated, beginGenerationTrace, setGenerationTraceId }: FlowTurnOptions) {
  const tabs = useTabManagerStore.getState();
  const sessionId = tabs.tabs.find(tab => tab.id === tabs.activeTabId)?.chatSessionId;
  const controller = new AbortController();
  flowRequest.current = controller;
  const userMessage: ChatMessage = { id: `flow-user-${Date.now()}`, type: 'user', content: inputValue.trim(), timestamp: new Date() };
  const progressId = `flow-progress-${Date.now()}`;
  setMessages(prev => [...prev, userMessage, { id: progressId, type: 'assistant', content: 'Finding saved crews and connecting your flow…', isIntermediate: true, timestamp: new Date() }]);
  setInputValue(''); setIsLoading(true);
  useUILayoutStore.getState().setFlowPanelTab('responses');
  try {
    await saveMessageToBackend(userMessage);
    const draft = await FlowService.generateFlow(userMessage.content, selectedModel, nodes.map(node => node.data?.crewId).filter(Boolean), controller.signal, beginGenerationTrace, sessionId);
    if (controller.signal.aborted) return;
    if (draft.nodes.length) onFlowGenerated?.(draft);
    const response: ChatMessage = {
      id: `flow-response-${Date.now()}`, type: 'assistant', timestamp: new Date(),
      metadata: draft.nodes.length ? { catalogKind: 'flow', catalogName: draft.name } : undefined,
      content: `${draft.nodes.length ? `**${draft.name}**\n\n` : ''}${draft.message}${draft.missing_capabilities?.length ? `\n\nNeeded: ${draft.missing_capabilities.join('; ')}` : ''}${draft.nodes.length ? '\n\nYour flow is on the canvas. Review the connections, then use Play to run it.' : ''}`,
    };
    setMessages(prev => [...prev.filter(message => message.id !== progressId), response]);
    await saveMessageToBackend(response);
  } catch (error) {
    if (!controller.signal.aborted) {
      const detail = (error as { response?: { data?: { detail?: unknown } } }).response?.data?.detail;
      const response: ChatMessage = { id: `flow-error-${Date.now()}`, type: 'assistant', timestamp: new Date(), content: typeof detail === 'string' ? detail : error instanceof Error ? error.message : 'Could not build the flow. Please try again.' };
      setMessages(prev => [...prev.filter(message => message.id !== progressId), response]);
      await saveMessageToBackend(response);
    }
  } finally {
    setMessages(prev => prev.filter(message => message.id !== progressId));
    if (flowRequest.current === controller) { flowRequest.current = null; setIsLoading(false); setGenerationTraceId(null); }
  }
}
