import { describe, expect, it } from 'vitest';
import { builderTranscript } from './builderTranscript';
import type { ChatMessage } from '../types';

describe('Builder activity placement', () => {
  it('shows a live run even before its first message or trace arrives', () => {
    expect(builderTranscript([], 'running-job')).toEqual([{ kind: 'activity', jobId: 'running-job' }]);
  });
  it('keeps each historical run’s activity before its output without duplicate trace bubbles', () => {
    const messages = [
      { id: 'prompt', type: 'user', content: 'Research' },
      { id: 'trace', type: 'trace', jobId: 'old', content: 'Tool call' },
      { id: 'answer', type: 'result', jobId: 'old', content: 'Report' },
      { id: 'trace2', type: 'trace', jobId: 'new', content: 'Model call' },
    ] as ChatMessage[];
    expect(builderTranscript(messages, 'new')).toEqual([
      { kind: 'message', message: messages[0] },
      { kind: 'activity', jobId: 'old' },
      { kind: 'message', message: messages[2] },
      { kind: 'activity', jobId: 'new' },
    ]);
  });
});
