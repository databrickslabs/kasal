import { expect, it, vi } from 'vitest';
import type { SetStateAction } from 'react';
import { FlowService } from '../../../../api/workflow/FlowService';
import type { ChatMessage } from '../types';
import { generateFlowTurn } from './generateFlowTurn';

vi.mock('../../../../api/workflow/FlowService', () => ({ FlowService: { generateFlow: vi.fn() } }));

it('persists the generation failure reason and clears progress without applying a canvas', async () => {
  vi.mocked(FlowService.generateFlow).mockRejectedValue(new Error('The model returned an incomplete flow plan.'));
  let messages: ChatMessage[] = [];
  const setMessages = (update: SetStateAction<ChatMessage[]>) => { messages = typeof update === 'function' ? update(messages) : update; };
  const setIsLoading = vi.fn();
  const saveMessageToBackend = vi.fn().mockResolvedValue(undefined);
  const onFlowGenerated = vi.fn();
  await generateFlowTurn({
    sessionId: 'flow-session',
    inputValue: 'Build a flow', selectedModel: 'test-model', nodes: [],
    flowRequest: { current: null }, setMessages, setInputValue: vi.fn(),
    setIsLoading, saveMessageToBackend, onFlowGenerated,
    beginGenerationTrace: vi.fn(), setGenerationTraceId: vi.fn(),
  });
  expect(messages.map(message => message.content)).toEqual(['Build a flow', 'The model returned an incomplete flow plan.']);
  expect(saveMessageToBackend).toHaveBeenLastCalledWith(expect.objectContaining({ content: 'The model returned an incomplete flow plan.' }));
  expect(setIsLoading).toHaveBeenLastCalledWith(false);
  expect(onFlowGenerated).not.toHaveBeenCalled();
});
