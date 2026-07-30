import { describe, expect, it, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useReasoningSupport, reasoningUnsupportedReason } from './useReasoningSupport';
import { useTabManagerStore } from '../../store/tabManager';

/**
 * Whether the reasoning control can do anything depends on the models the NEXT
 * run will actually use — and there are two sources, which is the whole reason
 * this has a test.
 *
 * The agents already on the canvas keep the llm they were created with. The
 * composer's selected model is what generation stamps on the agents it is about
 * to create. Counting only the first produced the reported bug: pick a GPT model
 * in the composer and the menu still named the canvas's stale Qwen agent as the
 * reason reasoning was unavailable — blaming a model the user had just replaced.
 */

const MODELS = {
  'gpt-5': { name: 'GPT-5', supports_reasoning_effort: true },
  'qwen-coder': { name: 'Qwen3-Coder-30B-A3B-Instruct', supports_reasoning_effort: false },
  'llama': { name: 'Llama', supports_reasoning_effort: false },
};

// The store's persist layer calls .toISOString() on these, so a bare {id, nodes}
// tab blows up before the hook is ever exercised.
const TAB_BASE = {
  id: 'tab-1',
  createdAt: new Date(),
  lastModified: new Date(),
};

function setCanvasAgents(models: string[]) {
  useTabManagerStore.setState({
    activeTabId: 'tab-1',
    tabs: [
      {
        ...TAB_BASE,
        nodes: models.map((llm, i) => ({
          id: `agent-${i}`,
          type: 'agentNode',
          position: { x: 0, y: 0 },
          data: { llm },
        })),
      },
    ],
  } as never);
}

describe('useReasoningSupport', () => {
  beforeEach(() => setCanvasAgents([]));

  it('counts the composer’s selected model', () => {
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'gpt-5'));
    expect(result.current.supported).toBe(true);
  });

  it('the selected model wins over a stale canvas agent — the reported bug', () => {
    setCanvasAgents(['qwen-coder']);
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'gpt-5'));
    expect(result.current.supported).toBe(true);
  });

  it('still counts canvas agents when they are the capable ones', () => {
    setCanvasAgents(['gpt-5']);
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'qwen-coder'));
    expect(result.current.supported).toBe(true);
  });

  it('is unsupported only when nothing in play has a budget', () => {
    setCanvasAgents(['qwen-coder', 'llama']);
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'llama'));
    expect(result.current.supported).toBe(false);
  });

  it('names every model in play, selected one included', () => {
    setCanvasAgents(['qwen-coder']);
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'llama'));
    expect(result.current.agentModelNames).toEqual(['llama', 'qwen-coder']);
  });

  it('does not duplicate a selected model that is also on the canvas', () => {
    setCanvasAgents(['qwen-coder']);
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'qwen-coder'));
    expect(result.current.agentModelNames).toEqual(['qwen-coder']);
  });

  it('an unknown model is treated as having no budget, not as an error', () => {
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'not-in-catalogue'));
    expect(result.current.supported).toBe(false);
  });

  it('ignores non-agent nodes', () => {
    useTabManagerStore.setState({
      activeTabId: 'tab-1',
      tabs: [
        {
          ...TAB_BASE,
          nodes: [
            { id: 't1', type: 'taskNode', position: { x: 0, y: 0 }, data: { llm: 'gpt-5' } },
          ],
        },
      ],
    } as never);
    const { result } = renderHook(() => useReasoningSupport(MODELS, 'qwen-coder'));
    expect(result.current.supported).toBe(false);
  });
});

describe('reasoningUnsupportedReason', () => {
  it('names the models so the message is actionable', () => {
    expect(reasoningUnsupportedReason(['Qwen'])).toContain('Qwen');
  });

  it('tells you what to do when there is nothing to name', () => {
    expect(reasoningUnsupportedReason([])).toContain('Add an agent');
  });
});
