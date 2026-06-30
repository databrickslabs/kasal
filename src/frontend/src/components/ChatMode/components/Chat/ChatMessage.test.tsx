import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatMessage, { TraceGroupMessage } from './ChatMessage';
import { CrewNameConflictError } from '../../api/crews';
import { useExecutionStore } from '../../store/executionStore';
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
  default: ({ value, onChange }: { value: string; onChange: (id: string) => void }) => (
    <div data-testid="genie-selector">
      {value}
      <button data-testid="genie-pick" onClick={() => onChange('space-123')}>pick</button>
    </div>
  ),
}));

vi.mock('../Cards/CrewActionsBar', () => ({
  default: ({ messageId }: { messageId: string }) => (
    <div data-testid="crew-actions-bar">{messageId}</div>
  ),
}));
vi.mock('../Cards/GenieSpacePrompt', () => ({
  default: () => <div data-testid="genie-space-prompt" />,
}));
vi.mock('../Cards/InputVariablesPrompt', () => ({
  default: ({ onSubmit }: { onSubmit?: (i: Record<string, string>) => void }) => (
    <button data-testid="input-vars-prompt" onClick={() => onSubmit?.({ topic: 'AI' })}>vars</button>
  ),
}));
vi.mock('../../../../shared/a2ui', async () => {
  const React = await import('react');
  return {
    A2UIRenderer: ({ payload }: { payload: { surfaceKind?: string } }) => (
      <div data-testid="a2ui-renderer">{payload?.surfaceKind}</div>
    ),
    DeckThemeContext: React.createContext({}),
    SurfaceChromeContext: React.createContext({ downloads: true }),
    getDeckTheme: () => ({ id: 'midnight' }),
    DEFAULT_DECK_THEME_ID: 'midnight',
    themeToDeck: (p: unknown) => p,
    themeToTokens: () => ({}),
  };
});
// Isolate ChatMessage from the workspace-themes fetch.
vi.mock('../../hooks/useA2uiThemes', () => ({ useA2uiThemes: () => null }));

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
  it('renders attachment chips on a user message', () => {
    render(
      <ChatMessage
        message={msg({ role: 'user', content: 'analyze these', attachments: ['a.txt', 'b.pdf'] })}
      />,
    );
    expect(screen.getByText('analyze these')).toBeInTheDocument();
    expect(screen.getByText('a.txt')).toBeInTheDocument();
    expect(screen.getByText('b.pdf')).toBeInTheDocument();
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
  it('crew_actions -> CrewActionsBar with the message id', () => {
    render(
      <ChatMessage
        message={msg({ id: 'm-7', resultType: 'crew_actions', resultData: { agents: [] } })}
      />,
    );
    expect(screen.getByTestId('crew-actions-bar')).toHaveTextContent('m-7');
  });
  it('genie_space_prompt -> GenieSpacePrompt', () => {
    render(
      <ChatMessage
        message={msg({ resultType: 'genie_space_prompt', resultData: { agents: [] } })}
      />,
    );
    expect(screen.getByTestId('genie-space-prompt')).toBeInTheDocument();
  });
  it('input_variables -> InputVariablesPrompt wiring onSubmitVariables with the message id', () => {
    const onSubmitVariables = vi.fn();
    render(
      <ChatMessage
        message={msg({
          id: 'm-9',
          resultType: 'input_variables',
          resultData: { variables: [{ name: 'topic', required: true }] },
        })}
        onSubmitVariables={onSubmitVariables}
      />,
    );
    fireEvent.click(screen.getByTestId('input-vars-prompt'));
    expect(onSubmitVariables).toHaveBeenCalledWith('m-9', { topic: 'AI' });
  });
  it('input_variables without variables renders nothing', () => {
    render(
      <ChatMessage message={msg({ resultType: 'input_variables', resultData: {} })} />,
    );
    expect(screen.queryByTestId('input-vars-prompt')).toBeNull();
  });
  it('input_variables without an onSubmitVariables handler submits safely', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'input_variables',
          resultData: { variables: [{ name: 'topic' }] },
        })}
      />,
    );
    fireEvent.click(screen.getByTestId('input-vars-prompt'));
  });
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
  it('catalog_load with plan -> renders the generated-style crew card (no bookmark, no crew-detail)', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'catalog_load',
          resultData: {
            plan: {
              id: 'p',
              name: 'My Crew',
              nodes: [{ id: 'agent-1', type: 'agentNode', data: { agentId: '1', label: 'Analyst', role: 'Data Analyst' } }],
              edges: [],
            },
          },
        })}
      />,
    );
    // Loaded crews render like generated crews (agent shown), not the technical
    // CrewDetailCard, and carry no save bookmark (they're already in the catalog).
    expect(screen.getByText('Analyst')).toBeInTheDocument();
    expect(screen.queryByTestId('crew-detail')).toBeNull();
  });
  it('catalog_load with plan converts nodes/edges to agents + tasks, exercising every name/id fallback', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'catalog_load',
          resultData: {
            plan: {
              id: 'p',
              name: 'My Crew',
              nodes: [
                // agent named via `label`, identified by `agentId`, full fields
                { id: 'agent-1', type: 'agentNode', data: { agentId: '1', label: 'Analyst', role: 'Data Analyst', goal: 'g', backstory: 'bg', tools: ['t1'] } },
                // agent with NO label → name falls back to `name`; id falls back to `id`
                { id: 'agent-2', type: 'agentNode', data: { id: '2', name: 'Writer' } },
                // agent with NO label/name → name falls back to `role`
                { id: 'agent-3', type: 'agentNode', data: { role: 'Reviewer' } },
                // agent with NO data at all → `n.data || {}` right side; name '' fallback
                { id: 'agent-4', type: 'agentNode' },
                // task owned by agent-1 via the agent-…→task-… edge; named via `label`
                { id: 'task-1', type: 'taskNode', data: { taskId: 't1', label: 'Crunch Numbers', description: 'd', expected_output: 'o', tools: ['t2'] } },
                // task with NO label → name falls back to `name`; orphan (no owning edge)
                { id: 'task-2', type: 'taskNode', data: { name: 'Lonely Task' } },
                // task with NO data at all → `n.data || {}` right side; name '' fallback; orphan
                { id: 'task-3', type: 'taskNode' },
                // a non-agent/non-task node is skipped by both loops
                { id: 'other-1', type: 'noteNode', data: { label: 'ignore me' } },
              ],
              edges: [
                { source: 'agent-1', target: 'task-1' },
                // edge with NO source → `e.source || ''` right side (still ignored for task-2)
                { target: 'task-2' },
                // a non-agent source edge into task-2 is ignored (stays orphan)
                { source: 'note-1', target: 'task-2' },
              ],
            },
          },
        })}
      />,
    );
    // Agent names render across all fallbacks; the linked + orphan tasks render too.
    expect(screen.getByText('Analyst')).toBeInTheDocument();
    expect(screen.getByText('Writer')).toBeInTheDocument();
    expect(screen.getByText('Reviewer')).toBeInTheDocument();
    expect(screen.getByText('Crunch Numbers')).toBeInTheDocument();
    expect(screen.getByText('Lonely Task')).toBeInTheDocument();
    // Orphan tasks (no owning agent) land under the "Other tasks" group.
    expect(screen.getByText('Other tasks')).toBeInTheDocument();
    expect(screen.queryByTestId('crew-detail')).toBeNull();
  });

  it('catalog_load with an empty plan (no nodes/edges) renders the generated card with no agents/tasks', () => {
    // `plan` is truthy but has neither nodes nor edges → planToGenerationData hits
    // the `plan?.nodes || []` / `plan?.edges || []` fallbacks and yields empty lists.
    render(<ChatMessage message={msg({ resultType: 'catalog_load', resultData: { plan: {} } })} />);
    expect(screen.queryByTestId('crew-detail')).toBeNull();
    expect(screen.queryByText('Other tasks')).not.toBeInTheDocument();
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
  it('a2ui -> renders the shared A2UI surface inline alongside the text', () => {
    render(
      <ChatMessage
        message={msg({
          content: 'Here is your deck.',
          resultType: 'a2ui',
          resultData: { surfaceKind: 'presentation', root: 'r', components: [], dataModel: {} },
        })}
      />,
    );
    // Both the prose AND the rendered surface show — inline-in-chat by default.
    expect(screen.getByTestId('content')).toHaveTextContent('Here is your deck.');
    expect(screen.getByTestId('a2ui-renderer')).toHaveTextContent('presentation');
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
  it('renders a Genie tool_result as a normal collapsed trace pill (no bespoke inline card)', () => {
    // Genie no longer has a special inline renderer — its answer flows through the
    // shared A2UI composer/surface like every other deliverable. The tool_result is
    // just the plumbing: a collapsed pill that expands to the raw detail on click.
    const genieDetail = [
      'Question:',
      'Top countries?',
      '',
      'Answer:',
      'Switzerland leads the customer base.',
    ].join('\n');
    render(
      <ChatMessage
        message={msg({
          resultType: 'trace',
          resultData: { label: 'GenieTool', kind: 'tool_result', durationMs: 4200, detail: genieDetail },
        })}
      />,
    );
    // Collapsed by default — the detail is hidden until the pill is expanded, and
    // there is no Genie-specific "Show question & SQL" inline affordance.
    expect(screen.queryByText(/Switzerland leads the customer base/)).not.toBeInTheDocument();
    expect(screen.queryByText('Show question & SQL')).not.toBeInTheDocument();
    // Expanding the GenieTool pill reveals the raw detail.
    fireEvent.click(screen.getByText('GenieTool').closest('button')!);
    expect(screen.getByText(/Switzerland leads the customer base/)).toBeInTheDocument();
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
  it('groups each task under its assigned agent with a per-agent task count', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: {
            agents: [
              { name: 'Researcher', role: 'Senior Researcher', goal: 'find', backstory: 'bg', tools: ['t1'] },
              { name: 'Writer' },
            ],
            tasks: [
              { name: 'Gather', assigned_agent: 'Researcher', description: 'desc', expected_output: 'out', tools: ['t2'] },
              { name: 'Review', assigned_agent: 'Researcher' },
              { name: 'Draft', assigned_agent: 'Writer' },
            ],
          },
        })}
      />,
    );
    // Combined summary, per-agent task badges (Researcher: 2, Writer: 1)
    expect(screen.getByText('2 Agents · 3 Tasks')).toBeInTheDocument();
    expect(screen.getByText('2 tasks')).toBeInTheDocument();
    expect(screen.getByText('1 task')).toBeInTheDocument();
    // role shown as a subtitle only when it differs from the name
    expect(screen.getByText('Senior Researcher')).toBeInTheDocument();
    // expand the agent + a nested task
    fireEvent.click(screen.getByText('Researcher').closest('button')!);
    expect(screen.getByText(/find/)).toBeInTheDocument();
    expect(screen.getByText(/bg/)).toBeInTheDocument();
    fireEvent.click(screen.getByText('Gather').closest('button')!);
    expect(screen.getByText('desc')).toBeInTheDocument();
    expect(screen.getByText('out')).toBeInTheDocument();
    // all tasks nested under their agent — no orphan section
    expect(screen.getByText('Review')).toBeInTheDocument();
    expect(screen.getByText('Draft')).toBeInTheDocument();
    expect(screen.queryByText('Other tasks')).not.toBeInTheDocument();
  });

  it('pluralizes the summary, uses index fallbacks, and lists agent-less tasks under "Other tasks"', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: { agents: [{}, {}], tasks: [{}, {}] },
        })}
      />,
    );
    expect(screen.getByText('2 Agents · 2 Tasks')).toBeInTheDocument();
    expect(screen.getByText('Agent 1')).toBeInTheDocument();
    expect(screen.getByText('Task 1')).toBeInTheDocument();
    // unassigned tasks grouped separately (agents present → "Other tasks")
    expect(screen.getByText('Other tasks')).toBeInTheDocument();
  });

  it('matches a task to its agent by agent_id or the "agent" field', () => {
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: {
            agents: [{ name: 'Alpha', id: 'a1', role: 'Alpha' }, { name: 'Beta' }],
            tasks: [{ name: 'ById', agent_id: 'a1' }, { name: 'ByAgentField', agent: 'Beta' }],
          },
        })}
      />,
    );
    expect(screen.getByText('ById')).toBeInTheDocument();
    expect(screen.getByText('ByAgentField')).toBeInTheDocument();
    expect(screen.queryByText('Other tasks')).not.toBeInTheDocument();
    // each agent resolves exactly one task
    expect(screen.getAllByText('1 task')).toHaveLength(2);
  });

  it('labels a tasks-only crew (no agents) with the task count — singular and plural', () => {
    const { unmount } = render(
      <ChatMessage
        message={msg({ resultType: 'generation_complete', resultData: { agents: [], tasks: [{ name: 'Solo' }] } })}
      />,
    );
    // no agents → orphan-section header uses the count, not "Other tasks"
    // (both the top summary and the orphan header render the singular label)
    expect(screen.getAllByText('1 Task').length).toBeGreaterThanOrEqual(2);
    expect(screen.queryByText('Other tasks')).not.toBeInTheDocument();
    unmount();

    render(
      <ChatMessage
        message={msg({ resultType: 'generation_complete', resultData: { agents: [], tasks: [{ name: 'One' }, { name: 'Two' }] } })}
      />,
    );
    expect(screen.getAllByText('2 Tasks').length).toBeGreaterThanOrEqual(2);
  });

  it('shows the Genie selector when an agent uses GenieTool (resolved via toolNameMap)', () => {
    toolNameMap = { '5': 'GenieTool' };
    render(
      <ChatMessage
        message={msg({
          resultType: 'generation_complete',
          resultData: { agents: [{ name: 'A', tools: ['5'] }], tasks: [] },
        })}
        onExecuteGenerated={vi.fn()}
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
        onExecuteGenerated={vi.fn()}
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

describe('ChatMessage — save crew bookmark', () => {
  const genMsg = () =>
    msg({
      resultType: 'generation_complete',
      resultData: { agents: [{ id: 'a1', name: 'A' }], tasks: [{ id: 't1', name: 'T' }] },
    });

  it('does not render the bookmark when onSaveCrew is not provided', () => {
    render(<ChatMessage message={genMsg()} />);
    expect(screen.queryByLabelText(/save crew to catalog/i)).not.toBeInTheDocument();
  });

  it('does not render the bookmark when there are no agents or tasks', () => {
    render(
      <ChatMessage
        message={msg({ resultType: 'generation_complete', resultData: { agents: [], tasks: [] } })}
        onSaveCrew={vi.fn()}
      />,
    );
    expect(screen.queryByLabelText(/save crew to catalog/i)).not.toBeInTheDocument();
  });

  it('saves on click and shows the saved confirmation, then ignores further clicks', async () => {
    const onSaveCrew = vi.fn().mockResolvedValue({ id: 'c1', name: 'My Crew' });
    render(<ChatMessage message={genMsg()} onSaveCrew={onSaveCrew} />);
    const btn = screen.getByLabelText('Save crew to catalog');
    fireEvent.click(btn);
    expect(await screen.findByText(/Saved .*My Crew.* to catalog/)).toBeInTheDocument();
    expect(onSaveCrew).toHaveBeenCalledTimes(1);
    // now disabled / labelled "Saved" — clicking again is a no-op
    const savedBtn = screen.getByLabelText('Saved to catalog');
    expect(savedBtn).toBeDisabled();
    fireEvent.click(savedBtn);
    expect(onSaveCrew).toHaveBeenCalledTimes(1);
  });

  it('ignores a second click while a save is in flight', async () => {
    let resolveSave: (v: { id: string; name: string }) => void = () => {};
    const onSaveCrew = vi.fn(
      () => new Promise<{ id: string; name: string }>((res) => { resolveSave = res; }),
    );
    render(<ChatMessage message={genMsg()} onSaveCrew={onSaveCrew} />);
    const btn = screen.getByLabelText('Save crew to catalog');
    fireEvent.click(btn); // starts saving (button now shows a spinner)
    fireEvent.click(btn); // in-flight → guard ignores it
    resolveSave({ id: 'c1', name: 'Done' });
    expect(await screen.findByText(/Saved .*Done.* to catalog/)).toBeInTheDocument();
    expect(onSaveCrew).toHaveBeenCalledTimes(1);
  });

  it('shows an error hint when saving fails', async () => {
    const onSaveCrew = vi.fn().mockRejectedValue(new Error('boom'));
    render(<ChatMessage message={genMsg()} onSaveCrew={onSaveCrew} />);
    fireEvent.click(screen.getByLabelText('Save crew to catalog'));
    expect(await screen.findByText(/Couldn.t save/i)).toBeInTheDocument();
    // still re-clickable after an error
    expect(screen.getByLabelText('Save crew to catalog')).not.toBeDisabled();
  });

  it('offers Overwrite when the name already exists, then retries with { overwrite: true }', async () => {
    const onSaveCrew = vi
      .fn()
      .mockRejectedValueOnce(new CrewNameConflictError('My Crew'))
      .mockResolvedValueOnce({ id: 'c1', name: 'My Crew' });
    render(<ChatMessage message={genMsg()} onSaveCrew={onSaveCrew} />);
    fireEvent.click(screen.getByLabelText('Save crew to catalog'));

    // conflict → "Already in catalog" hint + an Overwrite affordance
    expect(await screen.findByText(/Already in catalog/i)).toBeInTheDocument();
    const overwriteBtn = screen.getByText('Overwrite');
    expect(onSaveCrew).toHaveBeenNthCalledWith(1, expect.anything(), expect.not.objectContaining({ overwrite: true }));

    // clicking Overwrite re-saves, this time forcing overwrite
    fireEvent.click(overwriteBtn);
    expect(await screen.findByText(/Saved .*My Crew.* to catalog/)).toBeInTheDocument();
    expect(onSaveCrew).toHaveBeenCalledTimes(2);
    expect(onSaveCrew).toHaveBeenNthCalledWith(2, expect.anything(), expect.objectContaining({ overwrite: true }));
  });

  it('falls into the error state (not "exists") for a non-conflict rejection', async () => {
    const onSaveCrew = vi.fn().mockRejectedValue(new Error('boom'));
    render(<ChatMessage message={genMsg()} onSaveCrew={onSaveCrew} />);
    fireEvent.click(screen.getByLabelText('Save crew to catalog'));
    expect(await screen.findByText(/Couldn.t save/i)).toBeInTheDocument();
    expect(screen.queryByText(/Already in catalog/i)).not.toBeInTheDocument();
    expect(screen.queryByText('Overwrite')).not.toBeInTheDocument();
  });

  it('carries the selected Genie space into the save payload', async () => {
    const onSaveCrew = vi.fn().mockResolvedValue({ id: 'c1', name: 'Genie Crew' });
    render(
      <ChatMessage
        message={msg({
          // Unique id: genieSelectionStore is module-level + keyed by message id,
          // so a shared id would leak this picked space into sibling tests.
          id: 'genie-save-msg',
          resultType: 'generation_complete',
          resultData: { agents: [{ id: 'a1', name: 'A', tools: ['GenieTool'] }], tasks: [] },
        })}
        onSaveCrew={onSaveCrew}
        onExecuteGenerated={vi.fn()}
      />,
    );
    // Pick a Genie space, then save → the picked space rides along in the payload.
    fireEvent.click(screen.getByTestId('genie-pick'));
    fireEvent.click(screen.getByLabelText('Save crew to catalog'));
    expect(await screen.findByText(/Saved .*Genie Crew.* to catalog/)).toBeInTheDocument();
    expect(onSaveCrew).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ spaceId: 'space-123' }));
  });
});

describe('ChatMessage — Genie crew run button', () => {
  const genieMsg = () =>
    msg({
      resultType: 'generation_complete',
      resultData: { agents: [{ id: 'a1', name: 'A', tools: ['GenieTool'] }], tasks: [] },
    });

  it('shows the Run button (disabled) until a Genie space is selected, then runs with the space id', () => {
    const onExecuteGenerated = vi.fn();
    render(<ChatMessage message={genieMsg()} onExecuteGenerated={onExecuteGenerated} />);
    const runBtn = screen.getByText(/Select a Genie space to run/).closest('button')!;
    expect(runBtn).toBeDisabled();
    // clicking while disabled / no space does nothing
    fireEvent.click(runBtn);
    expect(onExecuteGenerated).not.toHaveBeenCalled();
    // pick a space via the selector's onChange — open it and choose
    // (GenieSpaceSelector is mocked to expose its value; drive onChange directly)
    fireEvent.click(screen.getByTestId('genie-pick'));
    const runBtn2 = screen.getByText('Run crew').closest('button')!;
    expect(runBtn2).not.toBeDisabled();
    fireEvent.click(runBtn2);
    expect(onExecuteGenerated).toHaveBeenCalledWith(expect.anything(), 'space-123');
    // becomes "Running…" and ignores a second click
    expect(screen.getByText('Running…')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Running…').closest('button')!);
    expect(onExecuteGenerated).toHaveBeenCalledTimes(1);
  });

  it('does not render the Run button when onExecute is not provided', () => {
    render(<ChatMessage message={genieMsg()} />);
    expect(screen.queryByText(/Run crew|Select a Genie space/)).not.toBeInTheDocument();
  });
});

describe('ChatMessage — a2ui preview-pane note', () => {
  // These tests drive the real executionStore preview state; reset it after each
  // so the inline-a2ui test elsewhere in this file keeps seeing a closed pane.
  afterEach(() => {
    useExecutionStore.setState({ previewPaneOpen: false, previewSourceMessageId: null });
  });

  const a2uiMsg = (id: string) =>
    msg({
      id,
      content: 'Here is your deck.',
      resultType: 'a2ui',
      resultData: { surfaceKind: 'presentation', root: 'r', components: [], dataModel: {} },
    });

  it('shows the "Opened in the side panel" note (not the inline surface) when the pane owns this message', () => {
    useExecutionStore.setState({ previewPaneOpen: true, previewSourceMessageId: 'm-side' });
    render(<ChatMessage message={a2uiMsg('m-side')} />);
    expect(screen.getByText('Opened in the side panel')).toBeInTheDocument();
    expect(screen.getByText('Show here')).toBeInTheDocument();
    // the inline surface is suppressed while it lives in the side pane
    expect(screen.queryByTestId('a2ui-renderer')).toBeNull();
  });

  it('still renders inline when the pane is open for a DIFFERENT message', () => {
    useExecutionStore.setState({ previewPaneOpen: true, previewSourceMessageId: 'other' });
    render(<ChatMessage message={a2uiMsg('m-mine')} />);
    expect(screen.getByTestId('a2ui-renderer')).toHaveTextContent('presentation');
    expect(screen.queryByText('Opened in the side panel')).toBeNull();
  });

  it('"Show here" closes the preview pane (clearPreview)', () => {
    useExecutionStore.setState({ previewPaneOpen: true, previewSourceMessageId: 'm-side' });
    render(<ChatMessage message={a2uiMsg('m-side')} />);
    fireEvent.click(screen.getByText('Show here'));
    expect(useExecutionStore.getState().previewPaneOpen).toBe(false);
  });

  it('the inline expand control opens the preview pane for this message', () => {
    const spy = vi.spyOn(useExecutionStore.getState(), 'openPreviewPane');
    render(<ChatMessage message={a2uiMsg('m-expand')} />);
    fireEvent.click(screen.getByLabelText('Open in preview pane'));
    expect(spy).toHaveBeenCalledWith(expect.anything(), 'm-expand');
    spy.mockRestore();
  });
});

describe('TraceGroupMessage (collapsed run of same-tool traces)', () => {
  it('summarizes a resolved run (check icon + total) and expands each call + detail', () => {
    const traces = [
      // resolved tool call: has sublabel (text=sublabel) + duration → header shows total + check icon
      { label: 'PerplexityTool', sublabel: 'first query', durationMs: 1200, kind: 'tool_call', detail: 'answer one' },
      // no sublabel (text falls back to label) and no duration (formatDurationMs → null);
      // kind is not a still-running tool_call so the group stays "done" (no spinner)
      { label: 'ScrapeTool', kind: 'memory' },
    ] as never;
    render(<TraceGroupMessage label="PerplexityTool" traces={traces} />);
    fireEvent.click(screen.getByText('PerplexityTool').closest('button')!);
    expect(screen.getByText('first query')).toBeInTheDocument();
    expect(screen.getByText('ScrapeTool')).toBeInTheDocument(); // sublabel || label fallback
    // expand the call that has a detail → its output renders
    fireEvent.click(screen.getByText('first query').closest('button')!);
    expect(screen.getByText('answer one')).toBeInTheDocument();
  });

  it('shows a spinner while any call is still pending (no duration yet)', () => {
    const pending = [{ label: 'X', sublabel: 'q', kind: 'tool_call' }] as never; // no durationMs
    render(<TraceGroupMessage label="X" traces={pending} />);
    expect(screen.getByTestId('trace-group-spinner')).toBeInTheDocument();
  });
});
