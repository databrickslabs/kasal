import type { ChatMessage } from '../types';

type TranscriptItem = { kind: 'message'; message: ChatMessage } | { kind: 'activity'; jobId: string };

/** Anchor one durable activity timeline at each run's first message. */
export function builderTranscript(messages: ChatMessage[], liveJobId?: string | null): TranscriptItem[] {
  const items: TranscriptItem[] = [];
  const seen = new Set<string>();
  for (const message of messages) {
    if (message.jobId && message.type !== 'user' && !seen.has(message.jobId)) {
      seen.add(message.jobId);
      items.push({ kind: 'activity', jobId: message.jobId });
    }
    // These events are represented by the activity header and durable trace.
    if (message.type === 'trace' || (message.type === 'execution' && (
      message.content.includes('🚀 Started execution:') ||
      message.content.includes('✅ Execution completed successfully') ||
      message.content.includes('⏳ Preparing to execute')
    ))) continue;
    items.push({ kind: 'message', message });
  }
  if (liveJobId && !seen.has(liveJobId)) items.push({ kind: 'activity', jobId: liveJobId });
  return items;
}
