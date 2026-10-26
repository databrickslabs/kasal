import { useEffect, useState } from 'react';
import { runService } from '../../../../api/execution/ExecutionHistoryService';
import { ChatHistoryServiceEnhanced } from '../../../../api/chat/ChatHistoryServiceEnhanced';
import { streamExecution } from '../../../chat/api/streaming';
import { toSurface } from '../../../chat/utils/surfaceAdapter';
import { useChatMessagesStore } from '../store/chatMessagesStore';
import { builderResultContent, hasBuilderDeck } from '../utils/resultContent';
import type { ChatMessage } from '../types';
import type { Surface } from '../../../../shared/a2ui';

/** Upgrade the existing transcript row; never append a second deliverable. */
function saveContent(messageId: string, content: string) {
  const store = useChatMessagesStore.getState();
  const entry = Object.entries(store.messagesBySession).find(([, messages]) =>
    messages.some(item => item.id === messageId));
  if (!entry) return;
  const current = entry[1].find(item => item.id === messageId)!;
  store.updateMessage(entry[0], messageId, { content });
  // In-flight saves attach their backend id and persist the latest content.
  if (current.backendId) {
    void ChatHistoryServiceEnhanced.updateMessageContent(current.backendId, content)
      .catch(error => console.warn('[Builder] Could not save updated surface', error));
  }
}

/** Crew completion precedes parent-side A2UI composition. Keep the answer
 * visible while waiting for the composed result, including without SSE. */
export function useBuilderResultSurface(message: ChatMessage) {
  const [resolved, setResolved] = useState<{ id: string; content: string } | null>(null);
  const content = resolved?.id === message.id ? resolved.content : message.content;
  const finalResult = message.type === 'result' && !message.isIntermediate;
  const hasDeck = hasBuilderDeck(content);
  const hasSurface = finalResult && Boolean(toSurface(content));
  const jobId = message.jobId;
  const messageId = message.id;
  const timestamp = message.timestamp.getTime();

  useEffect(() => {
    if (!finalResult || !jobId || hasSurface || hasDeck) return;
    let disposed = false;
    let complete = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // Old transcripts need one authoritative read; only recent completions
    // need polling while the parent composes. Bound retries for failed renders.
    const deadline = timestamp + 5 * 60_000;
    const accept = (result: unknown) => {
      if (disposed || complete || !toSurface(result)) return;
      const next = builderResultContent(result);
      if (!next) return;
      complete = true;
      setResolved({ id: messageId, content: next });
      saveContent(messageId, next);
      clearTimeout(timer);
    };
    const check = async () => {
      try { accept((await runService.getRunByJobId(jobId))?.result); }
      catch { /* Keep the text answer on transient transport failures. */ }
      if (!disposed && !complete && Date.now() < deadline) timer = setTimeout(check, 5000);
      else close();
    };
    const close = Date.now() < deadline ? streamExecution(jobId, event => {
      if (event.event === 'execution_update') accept(event.data.result);
    }) : () => {};
    void check();
    return () => { disposed = true; clearTimeout(timer); close(); };
  }, [messageId, timestamp, jobId, finalResult, hasSurface, hasDeck]);

  const restyle = (surface: Surface) => {
    let replaced = false;
    const replace = (value: unknown): unknown => {
      if (typeof value === 'string') {
        try {
          const parsed: unknown = JSON.parse(value);
          if (toSurface(parsed)) return replace(parsed);
        } catch { /* Plain answer text stays untouched. */ }
        return value;
      }
      if (value && typeof value === 'object') {
        const object = value as Record<string, unknown>;
        if (object.root && Array.isArray(object.components)) { replaced = true; return surface; }
        if (Array.isArray(value)) return value.map(replace);
        return Object.fromEntries(Object.entries(object).map(([key, item]) => [key, replace(item)]));
      }
      return value;
    };
    const updated = replace(content);
    const next = JSON.stringify(replaced ? updated : { text: content, a2ui: surface });
    setResolved({ id: messageId, content: next });
    saveContent(messageId, next);
  };
  // Deck edits also replace the resolved view, which may otherwise keep showing
  // an earlier composed result after the transcript message has been updated.
  const acceptContent = (next: string) => setResolved({ id: messageId, content: next });
  return { content, restyle, acceptContent };
}
