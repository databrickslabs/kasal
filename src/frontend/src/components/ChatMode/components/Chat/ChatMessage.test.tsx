import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatMessage from './ChatMessage';
import type { ChatMessage as ChatMessageType } from '../../types/chat';

// --- Mock leaf children so we isolate ChatMessage's routing logic ---
vi.mock('./MessageContent', () => ({
  default: ({ content }: { content: string }) => <div data-testid="content">{content}</div>,
}));
vi.mock('../Cards/AgentCard', () => ({ default: () => <div data-testid="agent-card" /> }));
vi.mock('../Cards/TaskCard', () => ({ default: () => <div data-testid="task-card" /> }));
vi.mock('../Cards/CrewListCard', () => ({
  default: ({ onCommand }: { onCommand?: (c: string) => void }) => (
    <button data-testid="crew-list" onClick={() => onCommand?.('/load crew x')}>crew-list</button>
  ),
}));
vi.mock('../Cards/FlowListCard', () => ({
  default: ({ onCommand }: { onCommand?: (c: string) => void }) => (
    <button data-testid="flow-list" onClick={() => onCommand?.('/load flow x')}>flow-list</button>
  ),
}));
vi.mock('../Cards/CrewDetailCard', () => ({
  default: ({ onExecute }: { onExecute?: () => void }) => (
    <button data-testid="crew-detail" disabled={!onExecute} onClick={() => onExecute?.()}>crew-detail</button>
  ),
}));
vi.mock('../Cards/FlowDetailCard', () => ({
  default: ({ onExecute }: { onExecute?: () => void }) => (
    <button data-testid="flow-detail" disabled={!onExecute} onClick={() => onExecute?.()}>flow-detail</button>
  ),
}));
vi.mock('../Cards/HelpCard', () => ({
  default: ({ content }: { content: string }) => <div data-testid="help-card">{content}</div>,
}));
vi.mock('../Cards/GenieSpaceSelector', () => ({
  default: ({ value }: { value: string }) => <div data-testid="genie-selector">{value}</div>,
}));

let toolNameMap: Record<string, string> = {};
vi.mock('../../store/appStore', () => ({
  useAppStore: (selector: (s: unknown) => unknown) => selector({ toolNameMap }),
}));

const msg = (over: Partial<ChatMessageType>): ChatMessageType =>
  ({ id: 'm', role: 'assistant', content: 'body', timestamp: new Date(), ...over } as ChatMessageType);

beforeEach(() => {
  toolNameMap = {};
});

describe('ChatMessage — roles', () => {
  it('renders a user bubble', () => {
    render(<ChatMessage message={msg({ role: 'user', content: 'hi user' })} />);
    expect(screen.getByText('hi user')).toBeInTheDocument();
  });
  it('renders a system pill', () => {
    render(<ChatMessage message={msg({ role: 'system', content: 'system note' })} />);
    expect(screen.getByText('system note')).toBeInTheDocument();
  });
  it('renders an assistant message with streaming indicator', () => {
    render(<ChatMessage message={msg({ role: 'assistant', content: 'thinking', isStreaming: true })} />);
    expect(screen.getByTestId('content')).toHaveTextContent('thinking');
  });
  it('renders an assistant message without rich content when no resultType', () => {
    render(<ChatMessage message={msg({ content: 'plain' })} />);
    expect(screen.getByTestId('content')).toHaveTextContent('plain');
    expect(screen.queryByTestId('agent-card')).not.toBeInTheDocument();
  });
});

describe('ChatMessage — rich content routing', () => {
  it('agent -> AgentCard', () => {
    render(<ChatMessage message={msg({ resultType: 'agent', resultData: { role: 'r' } })} />);
    expect(screen.getByTestId('agent-card')).toBeInTheDocument();
  });
  it('task -> TaskCard', () => {
    render(<ChatMessage message={msg({ resultType: 'task', resultData: { description: 'd' } })} />);
    expect(screen.getByTestId('task-card')).toBeInTheDocument();
  });
  it('catalog_list -> CrewListCard fires onCommand', () => {
    const onCommand = vi.fn();
    render(<ChatMessage message={msg({ resultType: 'catalog_list', resultData: { crews: [] } })} onCommand={onCommand} />);
    fireEvent.click(screen.getByTestId('crew-list'));
    expect(onCommand).toHaveBeenCalled();
  });
  it('catalog_load with plan -> CrewDetailCard fires onExecuteCrew', () => {
    const onExecuteCrew = vi.fn();
    render(
      <ChatMessage
        message={msg({ resultType: 'catalog_load', resultData: { plan: { id: 'p', nodes: [], edges: [] } } })}
        onExecuteCrew={onExecuteCrew}
      />,
    );
    fireEvent.click(screen.getByTestId('crew-detail'));
    expect(onExecuteCrew).toHaveBeenCalled();
  });
  it('catalog_load without plan -> CrewDetailCard with no execute', () => {
    render(<ChatMessage message={msg({ resultType: 'catalog_load', resultData: { plan: null } })} />);
    expect(screen.getByTestId('crew-detail')).toBeDisabled();
  });
  it('flow_list -> FlowListCard fires onCommand', () => {
    const onCommand = vi.fn();
    render(<ChatMessage message={msg({ resultType: 'flow_list', resultData: { flows: [] } })} onCommand={onCommand} />);
    fireEvent.click(screen.getByTestId('flow-list'));
    expect(onCommand).toHaveBeenCalled();
  });
  it('flow_load with flow -> FlowDetailCard fires onExecuteFlow', () => {
    const onExecuteFlow = vi.fn();
    render(
      <ChatMessage
        message={msg({ resultType: 'flow_load', resultData: { flow: { id: 'f', nodes: [], edges: [] } } })}
        onExecuteFlow={onExecuteFlow}
      />,
    );
    fireEvent.click(screen.getByTestId('flow-detail'));
    expect(onExecuteFlow).toHaveBeenCalled();
  });
  it('flow_load without flow -> FlowDetailCard disabled', () => {
    render(<ChatMessage message={msg({ resultType: 'flow_load', resultData: { flow: null } })} />);
    expect(screen.getByTestId('flow-detail')).toBeDisabled();
  });
  it('execute_crew with plan -> CrewDetailCard fires onExecuteCrew', () => {
    const onExecuteCrew = vi.fn();
    render(
      <ChatMessage
        message={msg({ resultType: 'execute_crew', resultData: { plan: { id: 'p', nodes: [], edges: [] } } })}
        onExecuteCrew={onExecuteCrew}
      />,
    );
    fireEvent.click(screen.getByTestId('crew-detail'));
    expect(onExecuteCrew).toHaveBeenCalled();
  });
  it('execute_crew without plan -> renders nothing', () => {
    render(<ChatMessage message={msg({ resultType: 'execute_crew', resultData: {} })} />);
    expect(screen.queryByTestId('crew-detail')).not.toBeInTheDocument();
  });
  it('execute_flow with flow -> FlowDetailCard fires onExecuteFlow', () => {
    const onExecuteFlow = vi.fn();
    render(
      <ChatMessage
        message={msg({ resultType: 'execute_flow', resultData: { flow: { id: 'f', nodes: [], edges: [] } } })}
        onExecuteFlow={onExecuteFlow}
      />,
    );
    fireEvent.click(screen.getByTestId('flow-detail'));
    expect(onExecuteFlow).toHaveBeenCalled();
  });
  it('execute_flow without flow -> renders nothing', () => {
    render(<ChatMessage message={msg({ resultType: 'execute_flow', resultData: {} })} />);
    expect(screen.queryByTestId('flow-detail')).not.toBeInTheDocument();
  });
  it('help with message uses the message', () => {
    render(<ChatMessage message={msg({ resultType: 'help', resultData: { message: 'help text' } })} />);
    expect(screen.getByTestId('help-card')).toHaveTextContent('help text');
  });
  it('help without message falls back to content', () => {
    render(<ChatMessage message={msg({ content: 'fallback help', resultType: 'help', resultData: {} })} />);
    expect(screen.getByTestId('help-card')).toHaveTextContent('fallback help');
  });
  it('unknown resultType -> renders nothing rich', () => {
    render(<ChatMessage message={msg({ resultType: 'something_else', resultData: { x: 1 } })} />);
    expect(screen.getByTestId('content')).toBeInTheDocument();
  });
  it('resultType present but resultData missing -> no rich content', () => {
    render(<ChatMessage message={msg({ resultType: 'agent', resultData: undefined })} />);
    expect(screen.queryByTestId('agent-card')).not.toBeInTheDocument();
  });
});

describe('ChatMessage — trace messages', () => {
  it('renders a pending tool_call with a spinner (no detail)', () => {
    render(<ChatMessage message={msg({ resultType: 'trace', resultData: { label: 'tool', kind: 'tool_call' } })} />);
    expect(screen.getByText('tool')).toBeInTheDocument();
  });
  it('renders a tool_result with ms duration and an expandable detail', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'trace',
          resultData: { label: 'search', kind: 'tool_result', durationMs: 250, sublabel: 'query', detail: 'big output' },
        })}
      />,
    );
    expect(screen.getByText(/250ms/)).toBeInTheDocument();
    expect(screen.getByText('query')).toBeInTheDocument();
    // detail collapsed initially; click to expand then collapse
    fireEvent.click(screen.getByText('search').closest('button')!);
    expect(screen.getByText('big output')).toBeInTheDocument();
    fireEvent.click(screen.getByText('search').closest('button')!);
  });
  it('renders an event with seconds duration and whitespace-only detail (not expandable)', () => {
    render(
      <ChatMessage
        message={msg({ resultType: 'trace', resultData: { label: 'evt', kind: 'event', durationMs: 1500, detail: '   ' } })}
      />,
    );
    expect(screen.getByText(/1\.50s/)).toBeInTheDocument();
    // not expandable -> clicking does nothing harmful
    fireEvent.click(screen.getByText('evt').closest('button')!);
    expect(screen.queryByText('   ')).not.toBeInTheDocument();
  });
});

describe('ChatMessage — generation_complete card', () => {
  it('renders agent/task rows and expands them', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: {
            agents: [{ name: 'Researcher', role: 'Researcher', goal: 'find', backstory: 'bg', tools: ['t1'] }],
            tasks: [{ name: 'Gather', description: 'desc', expected_output: 'out', tools: ['t2'] }],
          },
        })}
      />,
    );
    expect(screen.getByText('1 Agent')).toBeInTheDocument();
    expect(screen.getByText('1 Task')).toBeInTheDocument();
    // expand the agent row
    fireEvent.click(screen.getByText('Researcher').closest('button')!);
    expect(screen.getByText(/find/)).toBeInTheDocument();
    expect(screen.getByText(/bg/)).toBeInTheDocument();
    // expand the task row (use exact text to avoid matching the field labels)
    fireEvent.click(screen.getByText('Gather').closest('button')!);
    expect(screen.getByText('desc')).toBeInTheDocument();
    expect(screen.getByText('out')).toBeInTheDocument();
  });

  it('pluralizes multiple agents/tasks and uses index fallbacks for unnamed entries', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: { agents: [{}, {}], tasks: [{}, {}] },
        })}
      />,
    );
    expect(screen.getByText('2 Agents')).toBeInTheDocument();
    expect(screen.getByText('2 Tasks')).toBeInTheDocument();
    // index fallback names
    expect(screen.getByText('Agent 1')).toBeInTheDocument();
    expect(screen.getByText('Task 1')).toBeInTheDocument();
  });

  it('shows the Genie selector when an agent uses GenieTool (resolved via toolNameMap)', () => {
    toolNameMap = { '5': 'GenieTool' };
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: { agents: [{ name: 'A', tools: ['5'] }], tasks: [] },
        })}
      />,
    );
    expect(screen.getByTestId('genie-selector')).toBeInTheDocument();
  });

  it('shows the Genie selector when a task uses GenieTool directly', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: { agents: [], tasks: [{ name: 'T', tools: ['GenieTool'] }] },
        })}
      />,
    );
    expect(screen.getByTestId('genie-selector')).toBeInTheDocument();
  });

  it('normalizes generation data nested under result/data/generation_result', () => {
    const { rerender } = render(
      <ChatMessage message={msg({ resultType: 'generation_complete', resultData: { result: { agents: [{ name: 'X' }], tasks: [] } } })} />,
    );
    expect(screen.getByText('X')).toBeInTheDocument();
    rerender(<ChatMessage message={msg({ resultType: 'generation_complete', resultData: { data: { agents: [{ name: 'Y' }], tasks: [] } } })} />);
    expect(screen.getByText('Y')).toBeInTheDocument();
    rerender(<ChatMessage message={msg({ resultType: 'generation_complete', resultData: { generation_result: { agents: [{ name: 'Z' }], tasks: [] } } })} />);
    expect(screen.getByText('Z')).toBeInTheDocument();
  });

  it('handles empty/invalid generation data (no agents, no tasks)', () => {
    render(<ChatMessage message={msg({ resultType: 'generation_complete', resultData: null })} />);
    expect(screen.queryByText(/Agent/)).not.toBeInTheDocument();
    expect(screen.queryByTestId('genie-selector')).not.toBeInTheDocument();
  });

  it('handles truthy non-object generation data via normalizeGenerationData guard', () => {
    render(<ChatMessage message={msg({ resultType: 'generation_complete', resultData: 'not-an-object' as unknown as object })} />);
    expect(screen.queryByText(/Agent/)).not.toBeInTheDocument();
  });

  it('renders a role badge when the agent role differs from its name', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: { agents: [{ name: 'Alice', role: 'Senior Researcher', goal: 'g' }], tasks: [] },
        })}
      />,
    );
    expect(screen.getByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('Senior Researcher')).toBeInTheDocument();
  });

  it('does not expand rows that have no details', () => {
    render(
      <ChatMessage
        message={msg({ resultType: 'generation_complete', resultData: { agents: [{ name: 'NoDetail' }], tasks: [{ name: 'NoTaskDetail' }] } })}
      />,
    );
    // buttons are disabled (no details) — clicking does nothing
    const agentBtn = screen.getByText('NoDetail').closest('button')!;
    expect(agentBtn).toBeDisabled();
  });
});
