import { describe, it, expect } from 'vitest';
import type { ChatMessage } from './index';

describe('ChatMessage type', () => {
  it('accepts message without metadata', () => {
    const msg: ChatMessage = {
      id: '1',
      type: 'user',
      content: 'Hello',
      timestamp: new Date(),
    };
    expect(msg.metadata).toBeUndefined();
  });

  it('accepts message with metadata record', () => {
    const msg: ChatMessage = {
      id: '2',
      type: 'assistant',
      content: 'Response',
      timestamp: new Date(),
      metadata: { type: 'genie_config_needed', configs: [] },
    };
    expect(msg.metadata).toBeDefined();
    expect(msg.metadata?.type).toBe('genie_config_needed');
  });
});
