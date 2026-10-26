import { ChatHistoryServiceEnhanced } from '../../../../api/chat/ChatHistoryServiceEnhanced';
import { replaceDeckInContent, mergeDeckSegments, isDeck } from '../../../chat/utils/htmlDeck';
import { splitDiagramSegments } from '../../../chat/utils/mdSandboxDiagram';
import { useChatMessagesStore } from '../store/chatMessagesStore';
import { normalizeBuilderHtml } from './resultContent';

/** Replace only the selected deck, retaining result envelopes and A2UI siblings. */
export function replaceBuilderDeck(content: string, next: string, previous: string): string {
  let replaced = false;
  const replace = (value: unknown): unknown => {
    if (typeof value === 'string') {
      try {
        const parsed: unknown = JSON.parse(value);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          const result = replace(parsed);
          return replaced ? JSON.stringify(result) : value;
        }
      } catch { /* Plain HTML/markdown answer. */ }
      const normalized = normalizeBuilderHtml(value);
      const matches = mergeDeckSegments(splitDiagramSegments(normalized)).some(segment =>
        segment.type === 'diagram' && isDeck(segment.code) && segment.code.trim() === previous.trim());
      if (!replaced && matches) {
        replaced = true;
        return replaceDeckInContent(normalized, next, previous);
      }
      return value;
    }
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const result = { ...value } as Record<string, unknown>;
      // These are the answer fields understood by ChatMessageItem. Do not
      // rewrite strings inside generated UI components or other metadata.
      for (const key of ['text', 'value', 'result', 'output']) {
        if (!replaced && key in result) result[key] = replace(result[key]);
      }
      return result;
    }
    return value;
  };
  const updated = replace(content);
  if (!replaced) throw new Error('This deck changed. Reopen it before editing.');
  return updated as string;
}

/** Write to the captured owner, even if navigation changes during refinement. */
export async function saveBuilderDeck(
  sessionId: string, groupId: string, messageId: string, next: string, previous: string,
): Promise<string> {
  const store = useChatMessagesStore.getState();
  const message = store.messagesBySession[sessionId]?.find(item => item.id === messageId);
  if (!message?.backendId) throw new Error('The message has not been saved yet.');
  const content = replaceBuilderDeck(message.content, next, previous);
  await ChatHistoryServiceEnhanced.updateMessageContent(message.backendId, content, groupId);
  store.updateMessage(sessionId, messageId, { content });
  return content;
}
