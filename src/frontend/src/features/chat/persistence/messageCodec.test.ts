import { describe, expect, it } from 'vitest';
import { packExtras, toMessage, type MessageWire } from './messageCodec';

const wire: MessageWire = {
  id: 'message', session_id: 'session', message_type: 'assistant',
  content: 'short preview', timestamp: '2026-09-06T12:00:00',
};

describe('persisted message envelope', () => {
  it('round-trips attachments, run provenance and the complete answer', () => {
    const extras = {
      resultType: 'a2ui', resultData: { surfaceKind: 'document', components: [] },
      attachments: ['report.pdf'], images: [{ id: 'image', name: 'chart.png' }],
      executionId: 'job', usedWorkspaceMemory: false, capability: 'research',
      fullContent: 'the complete answer behind the short preview',
    };
    const restored = toMessage({ ...wire, generation_result: packExtras(extras) });
    expect(restored).toMatchObject(extras);
    expect(restored.timestamp.toISOString()).toBe('2026-09-06T12:00:00.000Z');
    expect(restored.isStreaming).toBe(false);
  });

  it('retains system messages and does not serialize transient state', () => {
    expect(toMessage({ ...wire, message_type: 'system' }).role).toBe('system');
    expect(packExtras({ isStreaming: true, content: 'transient' })).toBeUndefined();
  });

  it.each([
    [{ a2ui: { components: [] } }, 'a2ui'],
    [{ surfaceKind: 'document', components: [] }, 'a2ui'],
    [{ surfaceKind: 'document', components: null }, undefined],
    [{ a2ui: 'invalid' }, undefined],
    ['ordinary text', undefined],
    [null, undefined],
  ])('recovers surface cards while retaining other result payloads: %j', (resultData, resultType) => {
    const restored = toMessage({ ...wire, generation_result: { __chatmode: { resultData } } });
    expect(restored.resultType).toBe(resultType);
    expect(restored.resultData).toEqual(resultData);
  });
});
