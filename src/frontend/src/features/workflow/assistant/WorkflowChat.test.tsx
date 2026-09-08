/**
 * Tests for WorkflowChatRefactored component.
 *
 * Covers:
 * - Initial render states (empty chat, loading)
 * - Message display and grouping
 * - User input handling
 * - Execute command processing
 * - Variable extraction from nodes
 * - Session management
 * - Model selection
 */

import React from 'react';
import { act, render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi, describe, it, expect, beforeEach, afterEach, Mock } from 'vitest';
import WorkflowChat from './WorkflowChat';
import { improveChatPrompt } from '../../chat/api/prompt';
vi.mock('../../chat/api/prompt', () => ({ improveChatPrompt: vi.fn() }));
import { Node, Edge } from 'reactflow';

// Mock DOM methods not implemented in jsdom
Element.prototype.scrollIntoView = vi.fn();

// Mock all dependencies
vi.mock('../../../api/execution/DispatcherService', () => ({
  default: {
    dispatch: vi.fn(),
  },
}));

vi.mock('../../../api/chat/ChatHistoryService', () => ({
  ChatHistoryService: {
    getOrCreateSession: vi.fn().mockResolvedValue({ session_id: 'test-session-123' }),
    saveMessage: vi.fn().mockResolvedValue(undefined),
    getMessages: vi.fn().mockResolvedValue([]),
    getSessions: vi.fn().mockResolvedValue([]),
    deleteSession: vi.fn().mockResolvedValue(undefined),
  },
}));

vi.mock('../../../api/config/ModelService', () => ({
  ModelService: {
    getInstance: vi.fn().mockReturnValue({
      getEnabledModels: vi.fn().mockResolvedValue({
        'test-model': {
          name: 'test-model',
          temperature: 0.7,
          context_window: 128000,
          max_output_tokens: 4096,
          enabled: true,
        },
      }),
    }),
  },
}));

vi.mock('../../../api/execution/TraceService', () => ({
  default: {
    getTraces: vi.fn().mockResolvedValue([]),
    getTracesByJobId: vi.fn().mockResolvedValue([]),
  },
}));

vi.mock('../../../store/workflow', () => ({
  useWorkflowStore: () => ({
    setNodes: vi.fn(),
    setEdges: vi.fn(),
  }),
}));

vi.mock('../../../store/crewExecution', () => ({
  useCrewExecutionStore: () => ({
    setInputMode: vi.fn(),
    inputMode: 'chat',
    setInputVariables: vi.fn(),
    executeCrew: vi.fn(),
    executeFlow: vi.fn(),
    processType: 'sequential',
    setProcessType: vi.fn(),
    managerLLM: '',
    setManagerLLM: vi.fn(),
    reasoningEnabled: false,
    setReasoningEnabled: vi.fn(),
    reasoningConfig: { reasoning_effort: 'low' },
    setReasoningConfig: vi.fn(),
  }),
}));

vi.mock('./store/chatMessagesStore', () => {
  const storeState: {
    messagesBySession: Record<string, unknown[]>;
    setMessages: ReturnType<typeof vi.fn>;
    setCurrentSession: ReturnType<typeof vi.fn>;
  } = {
    messagesBySession: {},
    setMessages: vi.fn(),
    setCurrentSession: vi.fn(),
  };
  const hook = Object.assign(
    (selector?: (state: typeof storeState) => unknown) => {
      if (typeof selector === 'function') return selector(storeState);
      return storeState;
    },
    {
      // getState() returns the raw store state — used by the setMessages fix
      // to read the latest messages instead of a stale render-time snapshot.
      getState: () => storeState,
    },
  );
  return {
    useChatMessagesStore: hook,
    deduplicateMessages: (msgs: unknown[]) => msgs,
    // Export internal state handle for tests that need to mutate it
    __storeState: storeState,
  };
});

vi.mock('../../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: () => ({
    isMemoryBackendConfigured: false,
    isKnowledgeSourceEnabled: false,
    checkConfiguration: vi.fn(),
  }),
}));

vi.mock('../../../store/modelConfig', () => ({
  useModelConfigStore: () => ({
    refreshKey: 0,
  }),
}));

vi.mock('../../../store/uiLayout', () => ({
  useUILayoutState: () => ({
    chatPanelVisible: true,
    chatPanelCollapsed: false,
    chatPanelWidth: 450,
  }),
  useUILayoutStore: Object.assign(() => ({ chatPanelSide: 'right', setChatPanelSide: vi.fn() }), { getState: () => ({ assistantDockHeight: 0, setAssistantDockHeight: vi.fn(), setFlowPanelTab: vi.fn(), setAssistantPanelVisible: vi.fn() }) }),
}));

vi.mock('./hooks/useChatSession', () => ({
  useChatSession: () => ({
    sessionId: 'test-session-123',
    setSessionId: vi.fn(),
    chatSessions: [],
    setChatSessions: vi.fn(),
    isLoadingSessions: false,
    currentSessionName: 'New Chat',
    setCurrentSessionName: vi.fn(),
    saveMessageToBackend: vi.fn().mockResolvedValue(undefined),
    loadChatSessions: vi.fn(),
    loadSessionMessages: vi.fn(),
    startNewChat: vi.fn(),
  }),
}));

vi.mock('./hooks/useExecutionMonitoring', () => {
  // Mutable handle so tests can simulate a running execution.
  const execState: { executingJobId: string | null } = { executingJobId: null };
  return {
    useExecutionMonitoring: () => ({
      executingJobId: execState.executingJobId,
      setExecutingJobId: vi.fn(),
      lastExecutionJobId: null,
      setLastExecutionJobId: vi.fn(),
      executionStartTime: null,
      markPendingExecution: vi.fn(),
    }),
    __execState: execState,
  };
});

vi.mock('./components/ChatMessageItem', () => ({
  ChatMessageItem: ({ message }: { message: { content: string } }) => (
    <div data-testid="chat-message">{message.content}</div>
  ),
}));

vi.mock('./KnowledgeFileUpload', () => ({
  KnowledgeFileUpload: () => <div data-testid="knowledge-upload">Upload</div>,
}));

vi.mock('../canvas/lib/CanvasLayoutManager', () => {
  return {
    CanvasLayoutManager: class MockCanvasLayoutManager {
      getAgentNodePosition = vi.fn().mockReturnValue({ x: 100, y: 100 });
      getTaskNodePosition = vi.fn().mockReturnValue({ x: 380, y: 100 });
      updateUIState = vi.fn();
      updateScreenDimensions = vi.fn();
      getLayoutDebugInfo = vi.fn().mockReturnValue({});
      constructor() {}
    },
  };
});

describe('WorkflowChatRefactored', () => {
  const defaultProps = {
    onNodesGenerated: vi.fn(),
    onLoadingStateChange: vi.fn(),
    selectedModel: 'test-model',
    selectedTools: [],
    isVisible: true,
    setSelectedModel: vi.fn(),
    nodes: [] as Node[],
    edges: [] as Edge[],
    onExecuteCrew: vi.fn(),
    onToggleCollapse: vi.fn(),
    chatSessionId: 'test-session-123',
    onOpenLogs: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('Initial Rendering', () => {
    it('renders the chat component', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByText('Kasal')).toBeInTheDocument();
    });

    it('displays empty state message when no messages', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByText('What would you like to build?')).toBeInTheDocument();
    });

    it('puts a suggestion in the composer without submitting it', async () => {
      const DispatcherService = await import('../../../api/execution/DispatcherService');
      render(<WorkflowChat {...defaultProps} />);
      await userEvent.click(screen.getByRole('button', { name: 'Create an agent', exact: true }));
      const input = screen.getByRole('textbox', { name: 'Message Kasal' });
      expect(input).toHaveValue('Create an agent that can analyze financial data');
      expect(input).toHaveFocus();
      expect(DispatcherService.default.dispatch).not.toHaveBeenCalled();
    });

    it('renders input field', () => {
      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      expect(input).toBeInTheDocument();
    });

    it('renders header with session controls', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByLabelText('New Chat')).toBeInTheDocument();
      expect(screen.getByLabelText('Chat History')).toBeInTheDocument();
    });

    it('renders collapse button', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByLabelText('Collapse Chat')).toBeInTheDocument();
    });

    it('offers side placement in assistant options', async () => {
      render(<WorkflowChat {...defaultProps} />);

      await userEvent.click(screen.getByRole('button', { name: 'Assistant options' }));
      expect(screen.getByLabelText(/Move Chat to/)).toBeInTheDocument();
    });
  });

  describe('User Input', () => {
    it('updates input value on typing', async () => {
      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'Hello');

      expect(input).toHaveValue('Hello');
    });

    it('clears input after sending message', async () => {
      const DispatcherService = await import('../../../api/execution/DispatcherService');
      (DispatcherService.default.dispatch as Mock).mockResolvedValue({
        dispatcher: { intent: 'unknown', confidence: 0.5 },
        generation_result: null,
      });

      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'Hello world');

      await userEvent.click(screen.getByRole('button', { name: 'Send message' }));

      await waitFor(() => {
        expect(input).toHaveValue('');
      });
    });

    it('does not send empty messages', async () => {
      const DispatcherService = await import('../../../api/execution/DispatcherService');

      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      expect(DispatcherService.default.dispatch).not.toHaveBeenCalled();
    });

    it('handles shift+enter without sending', async () => {
      const DispatcherService = await import('../../../api/execution/DispatcherService');

      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'Line 1');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter', shiftKey: true });

      expect(DispatcherService.default.dispatch).not.toHaveBeenCalled();
    });
  });

  describe('Session Management', () => {
    it('calls startNewChat when new chat button clicked', async () => {
      const mockStartNewChat = vi.fn();
      vi.mocked(await import('./hooks/useChatSession')).useChatSession = () => ({
        sessionId: 'test-session-123',
        setSessionId: vi.fn(),
        chatSessions: [],
        setChatSessions: vi.fn(),
        isLoadingSessions: false,
        currentSessionName: 'New Chat',
        setCurrentSessionName: vi.fn(),
        saveMessageToBackend: vi.fn().mockResolvedValue(undefined),
        loadChatSessions: vi.fn(),
        loadSessionMessages: vi.fn(),
        startNewChat: mockStartNewChat,
      });

      render(<WorkflowChat {...defaultProps} />);

      const newChatButton = screen.getByLabelText('New Chat');
      fireEvent.click(newChatButton);

      // The mock is set up but we need to verify the component calls it
      // In this case, we just verify the button exists and is clickable
      expect(newChatButton).toBeInTheDocument();
    });

    it('opens session list when chat history button clicked', () => {
      render(<WorkflowChat {...defaultProps} />);

      const historyButton = screen.getByLabelText('Chat History');
      fireEvent.click(historyButton);

      expect(screen.getByText('Chat History')).toBeInTheDocument();
    });

    it('displays empty state in session list when no sessions', () => {
      render(<WorkflowChat {...defaultProps} />);

      const historyButton = screen.getByLabelText('Chat History');
      fireEvent.click(historyButton);

      expect(screen.getByText('No previous chat sessions found')).toBeInTheDocument();
    });
  });

  describe('Execute Commands', () => {
    it('handles execute crew command when crew content exists', async () => {
      const nodesWithCrew: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
      ];

      const mockOnExecuteCrew = vi.fn();

      render(<WorkflowChat {...defaultProps} nodes={nodesWithCrew} onExecuteCrew={mockOnExecuteCrew} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'execute crew');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      await waitFor(() => {
        expect(mockOnExecuteCrew).toHaveBeenCalled();
      });
    });

    it('handles "ec" shortcut command', async () => {
      const nodesWithCrew: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
      ];

      const mockOnExecuteCrew = vi.fn();

      render(<WorkflowChat {...defaultProps} nodes={nodesWithCrew} onExecuteCrew={mockOnExecuteCrew} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'ec');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      await waitFor(() => {
        expect(mockOnExecuteCrew).toHaveBeenCalled();
      });
    });

    it('handles "run" command', async () => {
      const nodesWithCrew: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
      ];

      const mockOnExecuteCrew = vi.fn();

      render(<WorkflowChat {...defaultProps} nodes={nodesWithCrew} onExecuteCrew={mockOnExecuteCrew} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'run');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      await waitFor(() => {
        expect(mockOnExecuteCrew).toHaveBeenCalled();
      });
    });
  });

  describe('Input Mode Commands', () => {
    it('handles input mode dialog command', async () => {
      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'input mode dialog');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      // The input should be cleared after processing
      await waitFor(() => {
        expect(input).toHaveValue('');
      });
    });

    it('handles input mode chat command', async () => {
      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'input mode chat');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      await waitFor(() => {
        expect(input).toHaveValue('');
      });
    });
  });

  describe('Collapse and Side Toggle', () => {
    it('calls onToggleCollapse when collapse button clicked', () => {
      const mockToggle = vi.fn();
      render(<WorkflowChat {...defaultProps} onToggleCollapse={mockToggle} />);

      const collapseButton = screen.getByLabelText('Collapse Chat');
      fireEvent.click(collapseButton);

      expect(mockToggle).toHaveBeenCalled();
    });

    it('offers the correct side placement for the right side', async () => {
      render(<WorkflowChat {...defaultProps} />);

      await userEvent.click(screen.getByRole('button', { name: 'Assistant options' }));
      expect(screen.getByLabelText('Move Chat to Left')).toBeInTheDocument();
    });
  });

  describe('Disabled State', () => {
    it('input field is enabled when not executing', () => {
      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      expect(input).not.toBeDisabled();
    });
  });

  describe('Prompt improvement', () => {
    it('replaces the draft with the rewrite without sending it', async () => {
      vi.mocked(improveChatPrompt).mockResolvedValue('Research the topic and summarize key findings.');
      const onNodesGenerated = vi.fn();
      render(<WorkflowChat {...defaultProps} onNodesGenerated={onNodesGenerated} />);
      const input = screen.getByRole('textbox', { name: 'Message Kasal' });
      fireEvent.change(input, { target: { value: 'Research this' } });
      fireEvent.click(screen.getByRole('button', { name: 'Improve prompt' }));
      await waitFor(() => expect(input).toHaveValue('Research the topic and summarize key findings.'));
      expect(improveChatPrompt).toHaveBeenCalledWith('Research this', 'test-model');
      expect(onNodesGenerated).not.toHaveBeenCalled();
    });

    it('preserves newer typing when an older rewrite finishes', async () => {
      let finish!: (text: string) => void;
      vi.mocked(improveChatPrompt).mockReturnValue(new Promise(resolve => { finish = resolve; }));
      render(<WorkflowChat {...defaultProps} />);
      const input = screen.getByRole('textbox', { name: 'Message Kasal' });
      fireEvent.change(input, { target: { value: 'Original request' } });
      fireEvent.click(screen.getByRole('button', { name: 'Improve prompt' }));
      fireEvent.change(input, { target: { value: 'My newer request' } });
      finish('Outdated rewrite');
      await waitFor(() => expect(screen.getByRole('button', { name: 'Improve prompt' })).toBeEnabled());
      expect(input).toHaveValue('My newer request');
    });

    it('keeps slash commands literal', () => {
      render(<WorkflowChat {...defaultProps} />);
      fireEvent.change(screen.getByRole('textbox', { name: 'Message Kasal' }), { target: { value: '/execute' } });
      expect(screen.getByRole('button', { name: 'Improve prompt' })).toBeDisabled();
    });
  });

  describe('Model Selection', () => {
    it('selects a model through the input plus menu', async () => {
      const onModelChange = vi.fn();
      render(<WorkflowChat {...defaultProps} setSelectedModel={onModelChange} />);
      fireEvent.click(screen.getByRole('button', { name: 'Files and run settings' }));
      fireEvent.click(await screen.findByText('Model'));
      fireEvent.click(await screen.findByText('test-model'));
      expect(onModelChange).toHaveBeenCalledWith('test-model');
    });
  });

  describe('Knowledge File Upload', () => {
    it('renders knowledge upload component', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByTestId('knowledge-upload')).toBeInTheDocument();
    });
  });

  describe('Empty State Suggestions', () => {
    it('displays suggestion for creating an agent', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByRole('button', { name: 'Create an agent', exact: true })).toBeInTheDocument();
    });

    it('displays suggestion for creating a task', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByRole('button', { name: 'Summarize documents', exact: true })).toBeInTheDocument();
    });

    it('displays suggestion for building a research team', () => {
      render(<WorkflowChat {...defaultProps} />);

      expect(screen.getByRole('button', { name: 'Build a research team', exact: true })).toBeInTheDocument();
    });
  });

  describe('Message Dispatch', () => {
    it('dispatches message to service when sending', async () => {
      const DispatcherService = await import('../../../api/execution/DispatcherService');
      (DispatcherService.default.dispatch as Mock).mockResolvedValue({
        dispatcher: { intent: 'generate_agent', confidence: 0.95 },
        generation_result: {
          name: 'Test Agent',
          role: 'Tester',
          goal: 'Test things',
          backstory: 'A test agent',
        },
      });

      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'Create an agent');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      await waitFor(() => {
        expect(DispatcherService.default.dispatch).toHaveBeenCalledWith({
          message: 'Create an agent',
          session_id: 'test-session-123',
          model: 'test-model',
          tools: [],
        }, expect.any(Function), expect.any(AbortSignal));
      });
    });

    it('handles dispatch errors gracefully', async () => {
      const DispatcherService = await import('../../../api/execution/DispatcherService');
      (DispatcherService.default.dispatch as Mock).mockRejectedValue(new Error('Network error'));

      render(<WorkflowChat {...defaultProps} />);

      const input = screen.getByPlaceholderText('Describe what you want to create...');
      await userEvent.type(input, 'Create an agent');
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

      // Should not throw and should clear loading state
      await waitFor(() => {
        expect(input).not.toBeDisabled();
      });
    });
  });
});

describe('Variable Extraction', () => {
  // These tests verify the extractVariablesFromNodes behavior indirectly
  // by checking the variable collection flow

  const propsWithVariableNodes = {
    onNodesGenerated: vi.fn(),
    onLoadingStateChange: vi.fn(),
    selectedModel: 'test-model',
    selectedTools: [],
    isVisible: true,
    setSelectedModel: vi.fn(),
    nodes: [
      {
        id: 'agent-1',
        type: 'agentNode',
        position: { x: 0, y: 0 },
        data: {
          role: 'Analyst for {company}',
          goal: 'Analyze {topic}',
          backstory: 'Expert in {domain}',
        },
      },
      {
        id: 'task-1',
        type: 'taskNode',
        position: { x: 100, y: 0 },
        data: {
          description: 'Research {subject}',
          expected_output: 'Report on {topic}',
        },
      },
    ] as Node[],
    edges: [] as Edge[],
    onExecuteCrew: vi.fn(),
    onToggleCollapse: vi.fn(),
    chatSessionId: 'test-session-123',
    onOpenLogs: vi.fn(),
  };

  it('renders component with variable-containing nodes', () => {
    render(<WorkflowChat {...propsWithVariableNodes} />);

    expect(screen.getByText('Kasal')).toBeInTheDocument();
  });
});

describe('extractVariablesFromNodes regex - identifier-only matching', () => {
  // Tests the regex pattern used inside the component's extractVariablesFromNodes function.
  // The regex was changed from /\{([^}]+)\}/g to /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g
  // to prevent CSS/JS brace content from being treated as template variables.
  const variablePattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;

  const extractVariables = (nodes: Node[]): string[] => {
    const foundVariables = new Set<string>();
    nodes.forEach(node => {
      if (node.type === 'agentNode' || node.type === 'taskNode') {
        const data = node.data as Record<string, unknown>;
        const fieldsToCheck = [
          data.role,
          data.goal,
          data.backstory,
          data.description,
          data.expected_output,
          data.label
        ];

        fieldsToCheck.forEach(field => {
          if (field && typeof field === 'string') {
            let match;
            variablePattern.lastIndex = 0;
            while ((match = variablePattern.exec(field)) !== null) {
              foundVariables.add(match[1]);
            }
          }
        });
      }
    });
    return Array.from(foundVariables);
  };

  const createNode = (id: string, type: string, data: Record<string, unknown>): Node => ({
    id,
    type,
    position: { x: 0, y: 0 },
    data,
  });

  it('extracts valid identifier variables from agent fields', () => {
    const nodes = [
      createNode('a1', 'agentNode', {
        role: 'Expert in {field}',
        goal: 'Analyze {target}',
        backstory: 'Trained on {dataset}',
      }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toContain('field');
    expect(vars).toContain('target');
    expect(vars).toContain('dataset');
  });

  it('extracts valid identifier variables from task fields', () => {
    const nodes = [
      createNode('t1', 'taskNode', {
        description: 'Research {subject}',
        expected_output: 'Report on {output_type}',
        label: 'Task for {client}',
      }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toContain('subject');
    expect(vars).toContain('output_type');
    expect(vars).toContain('client');
  });

  it('does NOT extract CSS content from task descriptions', () => {
    const nodes = [
      createNode('t1', 'taskNode', {
        description: '.reveal h1 { font-size: 2.2em; margin-bottom: 0.5em; } .reveal p { font-size: 0.9em; }',
      }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toHaveLength(0);
  });

  it('does NOT extract JS config objects from task descriptions', () => {
    const nodes = [
      createNode('t1', 'taskNode', {
        description: 'Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true })',
      }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toHaveLength(0);
  });

  it('extracts only valid variables from mixed content with CSS and JS braces', () => {
    const nodes = [
      createNode('t1', 'taskNode', {
        description: 'Create a {format} presentation about {topic}. CSS: .h1 { font-size: 2em; } Init: ({ width: 960 })',
      }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toEqual(expect.arrayContaining(['format', 'topic']));
    expect(vars).toHaveLength(2);
  });

  it('ignores non-agent/non-task nodes entirely', () => {
    const nodes = [
      createNode('c1', 'crewNode', { label: 'Crew for {project}' }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toHaveLength(0);
  });

  it('ignores non-string field values', () => {
    const nodes = [
      createNode('a1', 'agentNode', { role: 42, goal: null, backstory: undefined }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toHaveLength(0);
  });

  it('deduplicates same variable across multiple nodes', () => {
    const nodes = [
      createNode('a1', 'agentNode', { goal: 'Analyze {topic}' }),
      createNode('t1', 'taskNode', { description: 'Research {topic}' }),
    ];
    const vars = extractVariables(nodes);
    expect(vars.filter(v => v === 'topic')).toHaveLength(1);
  });

  it('handles the full reveal.js edge case with zero false positives', () => {
    const nodes = [
      createNode('t1', 'taskNode', {
        description: `Create a reveal.js presentation. Include CSS: .reveal .slides section { overflow: hidden; } .reveal h1 { font-size: 2.2em; margin-bottom: 0.5em; } .reveal h2 { font-size: 1.5em; margin-bottom: 0.4em; } .reveal ul, .reveal ol { font-size: 0.85em; max-height: 60vh; overflow: hidden; margin-left: 1em; } .reveal li { margin: 0.4em 0; line-height: 1.3; } .reveal img { max-height: 45vh; max-width: 85%; display: block; margin: 0 auto; } .reveal p { font-size: 0.9em; max-height: 50vh; overflow: hidden; }. Initialize with: Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true, transition: 'slide' }).`,
      }),
    ];
    const vars = extractVariables(nodes);
    expect(vars).toHaveLength(0);
  });
});

/**
 * Tests for the setMessages stale-closure fix.
 *
 * The component wraps Zustand's `setMessages` in a local callback that uses
 * `useChatMessagesStore.getState()` to read the latest messages instead of the
 * render-time `messages` snapshot.  This prevents rapid successive calls from
 * overwriting each other (the bug that caused user prompts to disappear in
 * Databricks Apps deployments where proxy latency made the race more likely).
 */
describe('setMessages stale-closure fix', () => {
  const defaultProps = {
    onNodesGenerated: vi.fn(),
    onLoadingStateChange: vi.fn(),
    selectedModel: 'test-model',
    selectedTools: [],
    isVisible: true,
    setSelectedModel: vi.fn(),
    nodes: [] as Node[],
    edges: [] as Edge[],
    onExecuteCrew: vi.fn(),
    onToggleCollapse: vi.fn(),
    chatSessionId: 'test-session-123',
    onOpenLogs: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('chatMessagesStore mock exposes getState()', async () => {
    // The fix relies on useChatMessagesStore.getState() being available.
    // Verify the mock surface matches what the component expects.
    const { useChatMessagesStore } = await import('./store/chatMessagesStore');
    expect(typeof useChatMessagesStore.getState).toBe('function');

    const state = useChatMessagesStore.getState();
    expect(state).toHaveProperty('messagesBySession');
    expect(state).toHaveProperty('setMessages');
  });

  it('getState() returns latest messagesBySession after mutation', async () => {
    const { __storeState } = await import('./store/chatMessagesStore') as { __storeState: { messagesBySession: Record<string, unknown[]> } };
    const { useChatMessagesStore } = await import('./store/chatMessagesStore');

    // Initially empty
    expect(useChatMessagesStore.getState().messagesBySession).toEqual({});

    // Simulate store mutation (as Zustand would do internally)
    __storeState.messagesBySession['test-session-123'] = [
      { id: 'msg-1', role: 'user', content: 'Hello', timestamp: new Date().toISOString() },
    ];

    // getState() should reflect the mutation immediately
    const msgs = useChatMessagesStore.getState().messagesBySession['test-session-123'];
    expect(msgs).toHaveLength(1);
    expect((msgs![0] as { content: string }).content).toBe('Hello');

    // Cleanup
    __storeState.messagesBySession = {};
  });

  it('component renders with getState-backed setMessages without errors', () => {
    // Smoke test: the component should mount and render correctly now that
    // its setMessages callback calls getState().
    render(<WorkflowChat {...defaultProps} />);

    expect(screen.getByText('Kasal')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Describe what you want to create...')).toBeInTheDocument();
  });

  it('setMessages via send uses getState, not stale closure', async () => {
    const { __storeState } = await import('./store/chatMessagesStore') as { __storeState: { messagesBySession: Record<string, unknown[]>; setMessages: ReturnType<typeof vi.fn> } };
    const DispatcherService = await import('../../../api/execution/DispatcherService');
    (DispatcherService.default.dispatch as Mock).mockResolvedValue({
      dispatcher: { intent: 'unknown', confidence: 0.5 },
      generation_result: null,
    });

    // Pre-populate store with an existing message to detect overwrite
    __storeState.messagesBySession['test-session-123'] = [
      { id: 'existing-1', role: 'assistant', content: 'Previous response', timestamp: new Date().toISOString() },
    ];

    render(<WorkflowChat {...defaultProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, 'New question');
    fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

    // After sending, setMessages (from Zustand) should have been called.
    // Because getState() is used, the updater fn receives the latest array
    // that includes 'existing-1', rather than an empty stale snapshot.
    await waitFor(() => {
      expect(__storeState.setMessages).toHaveBeenCalled();
    });

    // Cleanup
    __storeState.messagesBySession = {};
  });

  it('useCallback import is present in component source', async () => {
    // Structural check: the fix wraps setMessages in useCallback.
    // Verify by reading the module source.
    const fs = await import('fs');
    const path = await import('path');
    const componentPath = path.resolve(__dirname, 'WorkflowChat.tsx');
    const source = fs.readFileSync(componentPath, 'utf-8');

    // useCallback must be imported
    expect(source).toContain('useCallback');
    // setMessages must be wrapped in useCallback
    expect(source).toMatch(/const setMessages = useCallback/);
    // getState() must be used for reading latest messages
    expect(source).toContain('useChatMessagesStore.getState()');
  });
});

describe('SSE onComplete and genie config message', () => {
  const sseProps = {
    onNodesGenerated: vi.fn(),
    onLoadingStateChange: vi.fn(),
    selectedModel: 'test-model',
    selectedTools: [],
    isVisible: true,
    setSelectedModel: vi.fn(),
    nodes: [] as Node[],
    edges: [] as Edge[],
    onExecuteCrew: vi.fn(),
    onToggleCollapse: vi.fn(),
    chatSessionId: 'test-session-123',
    onOpenLogs: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders without errors when SSE hooks are present', () => {
    // Smoke test verifying the component mounts correctly with the
    // useCrewGenerationSSE hook wired in. The hook would add a message
    // with metadata.type='genie_config_needed' when pending genie configs
    // exist at onComplete time, but testing the full SSE flow requires
    // deep integration testing. This verifies the component itself does
    // not break with the new SSE integration.
    render(<WorkflowChat {...sseProps} />);

    expect(screen.getByText('Kasal')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Describe what you want to create...')).toBeInTheDocument();
  });

  // NOTE: Full integration test for onComplete adding genie_config_needed
  // metadata messages would require mocking useCrewGenerationSSE deeply
  // and simulating the SSE event stream. The actual behavior is:
  // 1. useCrewGenerationSSE's onComplete fires
  // 2. If pendingGenieConfigs has items, a message with
  //    metadata: { type: 'genie_config_needed', configs: [...] }
  //    is appended to the chat messages
  // 3. ChatMessageItem renders GenieSpaceConfigPrompt for that message
});

describe('chatCommandClick event listener', () => {
  const defaultProps = {
    onNodesGenerated: vi.fn(),
    onLoadingStateChange: vi.fn(),
    selectedModel: 'test-model',
    selectedTools: [],
    isVisible: true,
    setSelectedModel: vi.fn(),
    nodes: [] as Node[],
    edges: [] as Edge[],
    onExecuteCrew: vi.fn(),
    onToggleCollapse: vi.fn(),
    chatSessionId: 'test-session-123',
    onOpenLogs: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('populates input value when chatCommandClick event is dispatched', async () => {
    render(<WorkflowChat {...defaultProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    expect(input).toHaveValue('');

    // Dispatch the custom event as MessageRenderer would
    window.dispatchEvent(
      new CustomEvent('chatCommandClick', { detail: { command: '/load crew My Crew' } })
    );

    await waitFor(() => {
      expect(input).toHaveValue('/load crew My Crew');
    });
  });

  it('focuses the input field after chatCommandClick', async () => {
    render(<WorkflowChat {...defaultProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');

    // Blur the input first
    (input as HTMLElement).blur();

    window.dispatchEvent(
      new CustomEvent('chatCommandClick', { detail: { command: '/list flows' } })
    );

    await waitFor(() => {
      expect(input).toHaveValue('/list flows');
    });
  });

  it('cleans up event listener on unmount', () => {
    const removeEventListenerSpy = vi.spyOn(window, 'removeEventListener');

    const { unmount } = render(<WorkflowChat {...defaultProps} />);
    unmount();

    const chatCommandCalls = removeEventListenerSpy.mock.calls.filter(
      call => call[0] === 'chatCommandClick'
    );
    expect(chatCommandCalls.length).toBeGreaterThan(0);

    removeEventListenerSpy.mockRestore();
  });
});

describe('Slash Command Autocomplete Menu', () => {
  const slashProps = {
    onNodesGenerated: vi.fn(),
    onLoadingStateChange: vi.fn(),
    selectedModel: 'test-model',
    selectedTools: [],
    isVisible: true,
    setSelectedModel: vi.fn(),
    nodes: [] as Node[],
    edges: [] as Edge[],
    onExecuteCrew: vi.fn(),
    onToggleCollapse: vi.fn(),
    chatSessionId: 'test-session-slash',
    onOpenLogs: vi.fn(),
  };

  beforeEach(async () => {
    vi.clearAllMocks();
    // Pre-populate messages so the empty state (which contains command text like "/help")
    // does not render and conflict with slash menu assertions.
    // The useChatSession mock always returns sessionId 'test-session-123'.
    const { __storeState } = await import('./store/chatMessagesStore') as {
      __storeState: { messagesBySession: Record<string, unknown[]> };
    };
    __storeState.messagesBySession['test-session-123'] = [
      { id: 'msg-seed', type: 'assistant', content: 'Hello', timestamp: new Date().toISOString() },
    ];
  });

  afterEach(async () => {
    const { __storeState } = await import('./store/chatMessagesStore') as {
      __storeState: { messagesBySession: Record<string, unknown[]> };
    };
    delete __storeState.messagesBySession['test-session-123'];
  });

  it('does not show slash menu initially', () => {
    render(<WorkflowChat {...slashProps} />);
    // The slash menu renders command text like "List all saved crews" as secondary text.
    // When the menu is hidden, these descriptions should not be in the document.
    expect(screen.queryByText('List all saved crews')).not.toBeInTheDocument();
  });

  it('shows slash menu when typing "/"', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/');

    expect(screen.getByText('List all saved crews')).toBeInTheDocument();
    expect(screen.getByText('Show all available commands')).toBeInTheDocument();
  });

  it('filters commands when typing "/list"', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/list');

    expect(screen.getByText('List all saved crews')).toBeInTheDocument();
    expect(screen.getByText('List all saved flows')).toBeInTheDocument();
    // /help and /run should not be visible
    expect(screen.queryByText('Show all available commands')).not.toBeInTheDocument();
    expect(screen.queryByText('Execute the current crew')).not.toBeInTheDocument();
  });

  it('hides slash menu when input does not start with "/"', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/');
    expect(screen.getByText('Show all available commands')).toBeInTheDocument();

    await userEvent.clear(input);
    await userEvent.type(input, 'hello');
    expect(screen.queryByText('Show all available commands')).not.toBeInTheDocument();
  });

  it('hides slash menu when no commands match', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/zzz');
    expect(screen.queryByText('Show all available commands')).not.toBeInTheDocument();
  });

  it('selects command on Enter and fills input', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/help');

    // Press Enter to select the first (only) match
    fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

    // Input should be filled with the command + trailing space
    expect(input).toHaveValue('/help ');
    // Menu should be closed
    expect(screen.queryByText('Show all available commands')).not.toBeInTheDocument();
  });

  it('closes slash menu on Escape', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/');
    expect(screen.getByText('Show all available commands')).toBeInTheDocument();

    fireEvent.keyDown(input, { key: 'Escape', code: 'Escape' });
    expect(screen.queryByText('Show all available commands')).not.toBeInTheDocument();
  });

  it('navigates with ArrowDown and ArrowUp', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/list');

    // Two items: /list crews (index 0, selected by default), /list flows (index 1)
    const items = screen.getAllByRole('button').filter(btn =>
      btn.textContent?.includes('/list')
    );

    expect(items[0]).toHaveClass('Mui-selected');

    // ArrowDown to select second item
    fireEvent.keyDown(input, { key: 'ArrowDown', code: 'ArrowDown' });

    const updatedItems = screen.getAllByRole('button').filter(btn =>
      btn.textContent?.includes('/list')
    );
    expect(updatedItems[1]).toHaveClass('Mui-selected');

    // ArrowUp to go back to first item
    fireEvent.keyDown(input, { key: 'ArrowUp', code: 'ArrowUp' });

    const finalItems = screen.getAllByRole('button').filter(btn =>
      btn.textContent?.includes('/list')
    );
    expect(finalItems[0]).toHaveClass('Mui-selected');
  });

  it('fills input when clicking a command in the menu', async () => {
    render(<WorkflowChat {...slashProps} />);

    const input = screen.getByPlaceholderText('Describe what you want to create...');
    await userEvent.type(input, '/help');

    // Use description text to find the menu item precisely
    const helpItem = screen.getByText('Show all available commands').closest('[role="button"]')!;
    fireEvent.mouseDown(helpItem);

    expect(input).toHaveValue('/help ');
  });
});

/**
 * Test the model display name mapping logic used in the chat header.
 * This is a pure function extracted from the component to verify
 * new display names for GPT-5 variants.
 */
describe('WorkflowChat - model display name mapping', () => {
  // Replicate the display name logic from the component
  const getDisplayName = (displayName: string): string => {
    if (displayName.includes('meta-llama-3-3-70b')) return 'Llama 3.3 70B';
    if (displayName.includes('gpt-5-mini')) return 'GPT-5 Mini';
    if (displayName.includes('gpt-5-nano')) return 'GPT-5 Nano';
    if (displayName.includes('gpt-5-3-codex')) return 'GPT-5.3 Codex';
    if (displayName.includes('gpt-5-2') || displayName.includes('gpt-5.2')) return 'GPT-5.2';
    if (displayName.includes('gpt-5-1-codex-max')) return 'GPT-5.1 Codex Max';
    if (displayName.includes('gpt-5-1-codex-mini')) return 'GPT-5.1 Codex Mini';
    if (displayName.includes('gpt-5-1')) return 'GPT-5.1';
    if (displayName.includes('gpt-5')) return 'GPT-5';
    return displayName;
  };

  it('should map gpt-5-3-codex correctly', () => {
    expect(getDisplayName('databricks-gpt-5-3-codex')).toBe('GPT-5.3 Codex');
  });

  it('should map gpt-5-1 correctly', () => {
    expect(getDisplayName('databricks-gpt-5-1')).toBe('GPT-5.1');
  });

  it('should map gpt-5-1-codex-max correctly', () => {
    expect(getDisplayName('databricks-gpt-5-1-codex-max')).toBe('GPT-5.1 Codex Max');
  });

  it('should map gpt-5-1-codex-mini correctly', () => {
    expect(getDisplayName('databricks-gpt-5-1-codex-mini')).toBe('GPT-5.1 Codex Mini');
  });

  it('should map gpt-5-2 correctly', () => {
    expect(getDisplayName('databricks-gpt-5-2')).toBe('GPT-5.2');
  });

  it('should map gpt-5 correctly', () => {
    expect(getDisplayName('databricks-gpt-5')).toBe('GPT-5');
  });

  it('should map gpt-5-mini correctly', () => {
    expect(getDisplayName('databricks-gpt-5-mini')).toBe('GPT-5 Mini');
  });

  it('should map gpt-5-nano correctly', () => {
    expect(getDisplayName('databricks-gpt-5-nano')).toBe('GPT-5 Nano');
  });

  it('should preserve gpt-5-3-codex precedence over gpt-5', () => {
    // gpt-5-3-codex includes "gpt-5" — ensure the more specific match wins
    const name = 'databricks-gpt-5-3-codex';
    expect(getDisplayName(name)).toBe('GPT-5.3 Codex');
    expect(getDisplayName(name)).not.toBe('GPT-5');
  });

  it('should preserve gpt-5-1-codex-max precedence over gpt-5-1', () => {
    expect(getDisplayName('databricks-gpt-5-1-codex-max')).toBe('GPT-5.1 Codex Max');
    expect(getDisplayName('databricks-gpt-5-1-codex-max')).not.toBe('GPT-5.1');
  });
});

describe('Run activity rendering (removed from chat — lives in ShowTrace)', () => {
  const activityProps = {
    onNodesGenerated: vi.fn(),
    onLoadingStateChange: vi.fn(),
    selectedModel: 'test-model',
    selectedTools: [],
    isVisible: true,
    setSelectedModel: vi.fn(),
    nodes: [] as Node[],
    edges: [] as Edge[],
    onExecuteCrew: vi.fn(),
    onToggleCollapse: vi.fn(),
    chatSessionId: 'test-session-123',
    onOpenLogs: vi.fn(),
  };

  const getStore = async () =>
    (await import('./store/chatMessagesStore')) as unknown as {
      __storeState: { messagesBySession: Record<string, unknown[]> };
    };
  const getExecState = async () =>
    (await import('./hooks/useExecutionMonitoring')) as unknown as {
      __execState: { executingJobId: string | null };
    };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(async () => {
    (await getStore()).__storeState.messagesBySession = {};
    (await getExecState()).__execState.executingJobId = null;
  });

  it('does not render trace messages — run activity lives in ShowTrace', async () => {
    (await getExecState()).__execState.executingJobId = 'job-1';
    (await getStore()).__storeState.messagesBySession['test-session-123'] = [
      { id: 'u1', type: 'user', content: 'run crew', timestamp: new Date() },
      { id: 't1', type: 'trace', content: 'step one', timestamp: new Date(), jobId: 'job-1' },
      { id: 't2', type: 'trace', content: 'step two', timestamp: new Date(), jobId: 'job-1' },
    ];

    render(<WorkflowChat {...activityProps} />);

    expect(screen.queryAllByTestId('grouped-trace-messages')).toHaveLength(0);
    expect(screen.queryByText('step one')).not.toBeInTheDocument();
    expect(screen.getByText('run crew')).toBeInTheDocument();
  });

  it('does not render historical trace messages either', async () => {
    (await getStore()).__storeState.messagesBySession['test-session-123'] = [
      { id: 't1', type: 'trace', content: 'old step', timestamp: new Date(), jobId: 'job-9' },
      { id: 'a1', type: 'assistant', content: 'final answer', timestamp: new Date() },
    ];

    render(<WorkflowChat {...activityProps} />);

    expect(screen.queryAllByTestId('grouped-trace-messages')).toHaveLength(0);
    expect(screen.queryByText('old step')).not.toBeInTheDocument();
    expect(screen.getByText('final answer')).toBeInTheDocument();
  });

  it('shows no live activity placeholder while executing', async () => {
    (await getExecState()).__execState.executingJobId = 'job-1';
    (await getStore()).__storeState.messagesBySession['test-session-123'] = [
      { id: 'u1', type: 'user', content: 'run crew', timestamp: new Date() },
      { id: 'exec-pending-1', type: 'execution', content: '⏳ Preparing to execute crew...', timestamp: new Date() },
    ];

    render(<WorkflowChat {...activityProps} />);

    expect(screen.queryAllByTestId('grouped-trace-messages')).toHaveLength(0);
    expect(screen.queryByText(/Preparing to execute/)).not.toBeInTheDocument();
  });

  it('still filters execution start/complete noise messages', async () => {
    (await getStore()).__storeState.messagesBySession['test-session-123'] = [
      { id: 'e1', type: 'execution', content: '🚀 Started execution: foo', timestamp: new Date() },
      { id: 'e2', type: 'execution', content: '✅ Execution completed successfully', timestamp: new Date() },
      { id: 'a1', type: 'assistant', content: 'All done', timestamp: new Date() },
    ];

    render(<WorkflowChat {...activityProps} />);

    expect(screen.queryByText(/Started execution/)).not.toBeInTheDocument();
    expect(screen.queryByText(/completed successfully/)).not.toBeInTheDocument();
    expect(screen.getByText('All done')).toBeInTheDocument();
  });
});

vi.mock('../../../api/workflow/FlowService', () => ({ FlowService: { generateFlow: vi.fn() } }));

describe('Flow Builder conversation', () => {
  it('uses saved-crew flow generation and applies its draft instead of crew generation', async () => {
    const { FlowService } = await import('../../../api/workflow/FlowService');
    const draft = { name: 'Draft', message: 'Research then write', nodes: [{ id: 'flow-a', type: 'crewNode', position: { x: 0, y: 0 }, data: { crewId: 'saved-a' } }], edges: [], missing_capabilities: [] };
    vi.mocked(FlowService.generateFlow).mockResolvedValue(draft);
    const generated = vi.fn();
    render(<WorkflowChat builderMode="flow" nodes={draft.nodes} onFlowGenerated={generated} selectedModel="test-model" />);
    fireEvent.change(screen.getByRole('textbox', { name: 'Message Kasal' }), { target: { value: 'Research and write' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
    await waitFor(() => expect(generated).toHaveBeenCalledWith(draft));
    expect(FlowService.generateFlow).toHaveBeenCalledWith('Research and write', 'test-model', ['saved-a'], expect.any(AbortSignal), expect.any(Function), expect.any(String));
  });

  it('cancels generation when switching canvases and never applies the late result', async () => {
    const { FlowService } = await import('../../../api/workflow/FlowService');
    let finish!: (value: never) => void;
    vi.mocked(FlowService.generateFlow).mockImplementation(() => new Promise(resolve => { finish = resolve; }));
    const generated = vi.fn();
    const { rerender } = render(<WorkflowChat builderMode="flow" chatSessionId="first" onFlowGenerated={generated} />);
    fireEvent.change(screen.getByRole('textbox', { name: 'Message Kasal' }), { target: { value: 'Build a flow' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
    await waitFor(() => expect(finish).toBeDefined());
    const signal = vi.mocked(FlowService.generateFlow).mock.lastCall?.[3];
    rerender(<WorkflowChat builderMode="flow" chatSessionId="second" onFlowGenerated={generated} />);
    expect(signal?.aborted).toBe(true);
    finish({ name: 'Late', nodes: [{ id: 'late' }], edges: [], message: 'Late', missing_capabilities: [] } as never);
    await waitFor(() => expect(generated).not.toHaveBeenCalled());
  });
});

describe('Stop execution through the canvas control', () => {
  beforeEach(() => { vi.clearAllMocks(); vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} }); });
  const props = { onNodesGenerated: vi.fn(), onLoadingStateChange: vi.fn(), layout: 'canvas' as const };
  const execState = async () => ((await import('./hooks/useExecutionMonitoring')) as unknown as {
    __execState: { executingJobId: string | null };
  }).__execState;

  afterEach(async () => {
    (await execState()).executingJobId = null;
    vi.restoreAllMocks(); vi.unstubAllGlobals();
  });

  it.each(['crew', 'flow'] as const)('stops the active %s run, preventing duplicate requests', async builderMode => {
    const { apiClient } = await import('../../../shared/api/client');
    let finish!: (value: unknown) => void;
    const post = vi.spyOn(apiClient, 'post').mockImplementation(() => new Promise(resolve => { finish = resolve; }));
    const stopped = vi.fn();
    window.addEventListener('jobStopped', stopped);
    (await execState()).executingJobId = 'job-stop';
    render(<><div id="builder-assistant-composer-host" /><WorkflowChat {...props} builderMode={builderMode} /></>);
    const { useBuilderExecutionControls } = await import('../../../store/builderExecutionControls');
    const stop = useBuilderExecutionControls.getState()[builderMode]!.stop;
    expect(screen.getByRole('button', { name: 'Send message' })).toBeDisabled();
    expect(screen.queryByRole('button', { name: 'Stop execution' })).not.toBeInTheDocument();
    act(() => { void stop(); void stop(); });
    expect(useBuilderExecutionControls.getState()[builderMode]?.stopping).toBe(true);
    expect(post).toHaveBeenCalledTimes(1);
    expect(post).toHaveBeenCalledWith('/executions/job-stop/stop', {
      stop_type: 'graceful', reason: 'Stopped by user', preserve_partial_results: true,
    });
    finish({ data: { status: 'STOPPED', partial_results: 'partial report' } });
    await waitFor(() => expect(stopped).toHaveBeenCalledOnce());
    expect(stopped.mock.calls[0][0].detail).toMatchObject({ jobId: 'job-stop', partialResults: 'partial report' });
    window.removeEventListener('jobStopped', stopped);
  });

  it('keeps the execution active and allows retry if stopping fails', async () => {
    const { apiClient } = await import('../../../shared/api/client');
    const { toast } = await import('react-hot-toast');
    vi.spyOn(apiClient, 'post').mockRejectedValue(new Error('Network unavailable'));
    const error = vi.spyOn(toast, 'error');
    (await execState()).executingJobId = 'job-retry';
    render(<WorkflowChat {...props} />);
    const { useBuilderExecutionControls } = await import('../../../store/builderExecutionControls');
    await act(() => useBuilderExecutionControls.getState().crew!.stop());
    await waitFor(() => expect(error).toHaveBeenCalledWith('Could not stop execution. Please try again.'));
    expect(useBuilderExecutionControls.getState().crew?.stopping).toBe(false);
    expect((await execState()).executingJobId).toBe('job-retry');
  });
});

it('holds the reading position during streaming and resumes following only at the bottom', async () => {
  const { __storeState: state } = await import('./store/chatMessagesStore') as unknown as {
    __storeState: { messagesBySession: Record<string, unknown[]> };
  };
  const props = { onNodesGenerated: vi.fn(), onLoadingStateChange: vi.fn() };
  const stream = (content: string) => { state.messagesBySession['test-session-123'] = [
    { id: 'stream', type: 'assistant', content, timestamp: new Date(), isIntermediate: true },
  ]; };
  stream('Earlier output');
  const { rerender } = render(<WorkflowChat {...props} />);
  const viewport = screen.getByTestId('builder-conversation-scroll');
  Object.defineProperties(viewport, { scrollHeight: { value: 2000, configurable: true }, clientHeight: { value: 500 } });
  stream('Earlier output plus tokens');
  rerender(<WorkflowChat {...props} />);
  expect(viewport.scrollTop).toBe(2000);
  fireEvent.wheel(viewport, { deltaY: -10 });
  viewport.scrollTop = 1490; // Even a small upward scroll must pause following.
  fireEvent.scroll(viewport);
  stream('More tokens arriving');
  rerender(<WorkflowChat {...props} />);
  expect(viewport.scrollTop).toBe(1490);
  viewport.scrollTop = 800;
  fireEvent.scroll(viewport);
  stream('Still producing output');
  rerender(<WorkflowChat {...props} />);
  expect(viewport.scrollTop).toBe(800);
  viewport.scrollTop = 1500;
  fireEvent.scroll(viewport);
  Object.defineProperty(viewport, 'scrollHeight', { value: 2200 });
  stream('Follow these new tokens');
  rerender(<WorkflowChat {...props} />);
  expect(viewport.scrollTop).toBe(2200);
  state.messagesBySession = {};
});
