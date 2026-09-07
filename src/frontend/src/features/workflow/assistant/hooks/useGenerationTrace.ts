import { useCallback, useRef, useState, type Dispatch, type SetStateAction } from 'react';
import type { ChatMessage } from '../types';

/** Anchor generation activity to the transcript independently of workload execution. */
export function useGenerationTrace(setMessages: Dispatch<SetStateAction<ChatMessage[]>>, saveMessage: (message: ChatMessage) => Promise<void>) {
  const generationCompletedRef = useRef(false);
  const [generationTraceId, setGenerationTraceId] = useState<string | null>(null);
  const beginGenerationTrace = useCallback((jobId: string) => {
    generationCompletedRef.current = false;
    setGenerationTraceId(jobId);
    const anchor: ChatMessage = { id: `generation-trace-${jobId}`, type: 'trace', jobId, content: 'Design activity', timestamp: new Date() };
    setMessages(prev => [...prev, anchor]);
    void saveMessage(anchor);
  }, [setMessages, saveMessage]);
  return { generationCompletedRef, generationTraceId, setGenerationTraceId, beginGenerationTrace };
}
