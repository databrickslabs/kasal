/** Backend wire contracts and persisted chat-message envelope mapping. */
import type { ChatMessage, ChatSession } from '../types/chat';

export interface NamedSessionWire {
  mode?: 'chat' | 'crew' | 'flow';
  id: string;
  title: string;
  user_id: string;
  group_id?: string | null;
  created_at: string;
  updated_at: string;
}

export interface MessageWire {
  id: string;
  session_id: string;
  message_type: string;
  content: string;
  intent?: string | null;
  generation_result?: Record<string, unknown> | null;
  timestamp: string;
}

/** Backend timestamps are naive UTC — parse them as UTC, not local time. */
const parseUtc = (iso: string): Date =>
  new Date(/[zZ]|[+-]\d{2}:?\d{2}$/.test(iso) ? iso : `${iso}Z`);

export const toSession = (w: NamedSessionWire): ChatSession => ({
  id: w.id,
  title: w.title,
  createdAt: parseUtc(w.created_at),
  updatedAt: parseUtc(w.updated_at),
  ...(w.group_id ? { groupId: w.group_id } : {}),
  ...(w.mode && w.mode !== 'chat' ? { mode: w.mode } : {}),
});

// ChatMode-specific message fields ride in generation_result under this key
// so the column stays compatible with the sidebar chat's usage.
const EXTRA_KEY = '__chatmode';

interface ChatModeExtras {
  resultType?: string;
  resultData?: unknown;
  attachments?: string[];
  images?: ChatMessage['images'];
  executionId?: string;
  usedWorkspaceMemory?: boolean;
  /**
   * For an answer produced by a routed run: which published capability produced
   * it. Read back by the BACKEND router on the next turn — without it the
   * router cannot tell that the answer on screen came from a capability at all,
   * so a follow-up is re-matched from scratch and a flow mid-conversation
   * quietly loses the thread.
   */
  capability?: string;
  /** Uncapped text behind a capped step preview (see ChatMessage.fullContent). */
  fullContent?: string;
}

/** True when a stored resultData is (or wraps) an A2UI surface — used to heal
 *  messages whose `__chatmode` envelope was clobbered by an old partial update
 *  (only `resultData` survived): they must still render as an a2ui card. */
const looksLikeSurface = (v: unknown): boolean => {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  if (typeof o.surfaceKind === 'string' && Array.isArray(o.components)) return true;
  return Boolean(o.a2ui && typeof o.a2ui === 'object');
};

export const toMessage = (w: MessageWire): ChatMessage => {
  const extras = (w.generation_result?.[EXTRA_KEY] || {}) as ChatModeExtras;
  const role: ChatMessage['role'] =
    w.message_type === 'user' || w.message_type === 'system'
      ? (w.message_type as ChatMessage['role'])
      : 'assistant';
  // Heal envelope-clobbered rows: a surface-shaped resultData without a
  // resultType is an a2ui card whose type was stripped by a partial PUT.
  const resultType =
    extras.resultType ?? (looksLikeSurface(extras.resultData) ? 'a2ui' : undefined);
  return {
    id: w.id,
    sessionId: w.session_id,
    role,
    content: w.content === '[ui-card]' ? '' : w.content,
    timestamp: parseUtc(w.timestamp),
    ...(w.intent ? { intent: w.intent as ChatMessage['intent'] } : {}),
    ...(resultType ? { resultType } : {}),
    ...(extras.resultData !== undefined ? { resultData: extras.resultData } : {}),
    ...(extras.attachments ? { attachments: extras.attachments } : {}),
    ...(extras.images ? { images: extras.images } : {}),
    ...(extras.executionId ? { executionId: extras.executionId } : {}),
    ...(extras.usedWorkspaceMemory !== undefined
      ? { usedWorkspaceMemory: extras.usedWorkspaceMemory }
      : {}),
    ...(extras.capability ? { capability: extras.capability } : {}),
    ...(extras.fullContent ? { fullContent: extras.fullContent } : {}),
    isStreaming: false,
  };
};

export const packExtras = (msg: Partial<ChatMessage>): Record<string, unknown> | undefined => {
  const extras: ChatModeExtras = {};
  if (msg.resultType !== undefined) extras.resultType = msg.resultType;
  if (msg.resultData !== undefined) extras.resultData = msg.resultData;
  if (msg.attachments !== undefined) extras.attachments = msg.attachments;
  if (msg.images !== undefined) extras.images = msg.images;
  if (msg.executionId !== undefined) extras.executionId = msg.executionId;
  if (msg.usedWorkspaceMemory !== undefined) extras.usedWorkspaceMemory = msg.usedWorkspaceMemory;
  if (msg.capability !== undefined) extras.capability = msg.capability;
  if (msg.fullContent !== undefined) extras.fullContent = msg.fullContent;
  return Object.keys(extras).length > 0 ? { [EXTRA_KEY]: extras } : undefined;
};
