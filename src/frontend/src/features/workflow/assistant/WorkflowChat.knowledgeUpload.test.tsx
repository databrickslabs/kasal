/**
 * Attaching knowledge files from the canvas composer.
 *
 * KnowledgeFileUpload runs its whole per-file loop with the callbacks of the
 * render in which the files were picked. Those callbacks must work from the
 * canvas as it is now, not as it was at pick time: otherwise only the last file
 * stays searchable and edits made while a file embeds are reverted.
 */
import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Node } from 'reactflow';
import WorkflowChat from './WorkflowChat';
import { useWorkflowStore } from '../../../store/workflow';
import { useBuilderCanvasStore } from '../../../app/sessions/builderCanvasStore';
import { TaskService } from '../../../api/workflow/TaskService';
import type { Agent } from '../../../api/workflow/AgentService';

Element.prototype.scrollIntoView = vi.fn();

const upload: {
  onTasksUpdated?: (path: string) => void | Promise<void>;
  onAgentsUpdated?: (agents: Agent[]) => void;
} = {};

vi.mock('./KnowledgeFileUpload', () => ({
  KnowledgeFileUpload: (props: typeof upload) => {
    // Keep the FIRST render's callbacks, as the upload loop does.
    upload.onTasksUpdated ??= props.onTasksUpdated;
    upload.onAgentsUpdated ??= props.onAgentsUpdated;
    return null;
  },
}));
vi.mock('../../../api/workflow/TaskService', () => ({ TaskService: { updateTask: vi.fn().mockResolvedValue({}) } }));
vi.mock('../../chat/api/prompt', () => ({ improveChatPrompt: vi.fn() }));
vi.mock('react-hot-toast', () => ({ default: Object.assign(vi.fn(), { error: vi.fn(), success: vi.fn() }), toast: Object.assign(vi.fn(), { error: vi.fn(), success: vi.fn() }) }));
vi.mock('../../../api/execution/DispatcherService', () => ({ default: { dispatch: vi.fn() } }));
vi.mock('../../../api/chat/ChatHistoryService', () => ({
  ChatHistoryService: {
    getOrCreateSession: vi.fn().mockResolvedValue({ session_id: 'session' }),
    saveMessage: vi.fn().mockResolvedValue(undefined),
    getMessages: vi.fn().mockResolvedValue([]),
    getSessions: vi.fn().mockResolvedValue([]),
  },
}));
vi.mock('../../../api/config/EnabledModelsService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../../api/config/EnabledModelsService')>()),
  fetchEnabledModelRows: vi.fn().mockResolvedValue([]),
}));
vi.mock('../../../api/execution/TraceService', () => ({
  default: { getTraces: vi.fn().mockResolvedValue([]), getTracesByJobId: vi.fn().mockResolvedValue([]) },
}));
vi.mock('./hooks/useChatSession', () => ({
  useChatSession: () => ({
    sessionId: 'session', setSessionId: vi.fn(), chatSessions: [], setChatSessions: vi.fn(),
    isLoadingSessions: false, currentSessionName: 'New Chat', setCurrentSessionName: vi.fn(),
    saveMessageToBackend: vi.fn().mockResolvedValue(undefined), loadChatSessions: vi.fn(),
    loadSessionMessages: vi.fn(), startNewChat: vi.fn(),
  }),
}));
vi.mock('./hooks/useExecutionMonitoring', () => ({
  useExecutionMonitoring: () => ({
    executingJobId: null, setExecutingJobId: vi.fn(), lastExecutionJobId: null,
    setLastExecutionJobId: vi.fn(), executionStartTime: null, markPendingExecution: vi.fn(),
  }),
}));

const KNOWLEDGE = 'DatabricksKnowledgeSearchTool';
const agentNode: Node = { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: { agentId: 'a1', label: 'Researcher', tools: [] } };
const taskNode: Node = {
  id: 'task-1', type: 'taskNode', position: { x: 0, y: 0 },
  data: { taskId: 't1', label: 'Research', tools: ['SerperDevTool'], tool_configs: { SerperDevTool: { n_results: 5 } } },
};

const liveTask = () => useWorkflowStore.getState().nodes.find(node => node.id === 'task-1')!;
const lastTaskWrite = () => vi.mocked(TaskService.updateTask).mock.lastCall?.[1];

describe('WorkflowChat knowledge upload wiring', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    delete upload.onTasksUpdated;
    delete upload.onAgentsUpdated;
    useWorkflowStore.setState({ nodes: [agentNode, taskNode], edges: [] });
    useBuilderCanvasStore.setState({ activeCanvasId: 'canvas-1' });
  });

  const renderChat = () => render(<WorkflowChat nodes={useWorkflowStore.getState().nodes} edges={[]} chatSessionId="session" />);

  it('keeps every uploaded file and the edits made while the files were uploading', async () => {
    renderChat();
    expect(upload.onTasksUpdated).toBeTypeOf('function');

    await upload.onTasksUpdated!('uploads/g/s/first.pdf');
    // The user keeps working while the second file embeds.
    const added: Node = { id: 'task-2', type: 'taskNode', position: { x: 300, y: 0 }, data: { label: 'Added meanwhile' } };
    useWorkflowStore.getState().setNodes(nodes => [...nodes.map(node => node.id === 'agent-1'
      ? { ...node, position: { x: 50, y: 60 } } : node), added]);
    upload.onAgentsUpdated!([{ id: 'a1', name: 'Researcher', knowledge_sources: [{ source: 'uploads/g/s/first.pdf' }] } as unknown as Agent]);
    await upload.onTasksUpdated!('uploads/g/s/second.pdf');

    const config = (liveTask().data.tool_configs as Record<string, { file_paths?: string[] }>)[KNOWLEDGE];
    expect(config.file_paths).toEqual(['uploads/g/s/first.pdf', 'uploads/g/s/second.pdf']);
    expect(liveTask().data.tool_configs.SerperDevTool).toEqual({ n_results: 5 });
    expect(useWorkflowStore.getState().nodes.map(node => node.id)).toContain('task-2');
    expect(useWorkflowStore.getState().nodes.find(node => node.id === 'agent-1')!.position).toEqual({ x: 50, y: 60 });

    await waitFor(() => expect(lastTaskWrite()?.tool_configs?.[KNOWLEDGE]).toEqual(
      expect.objectContaining({ file_paths: ['uploads/g/s/first.pdf', 'uploads/g/s/second.pdf'] })));
    expect(Object.keys(lastTaskWrite()!).sort()).toEqual(['tool_configs', 'tools']);
  });

  it('does not wire a file into another canvas opened while it was embedding', async () => {
    renderChat();
    // The user switches canvas; the store now holds the other crew.
    const otherTask: Node = { id: 'task-9', type: 'taskNode', position: { x: 0, y: 0 }, data: { taskId: 't9', label: 'Other', tools: [] } };
    useBuilderCanvasStore.setState({ activeCanvasId: 'canvas-2' });
    useWorkflowStore.setState({ nodes: [otherTask] });

    await upload.onTasksUpdated!('uploads/g/s/late.pdf');
    upload.onAgentsUpdated!([{ id: 'a1', name: 'Researcher', knowledge_sources: [{ source: 'uploads/g/s/late.pdf' }] } as unknown as Agent]);

    expect(useWorkflowStore.getState().nodes).toEqual([otherTask]);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(TaskService.updateTask).not.toHaveBeenCalled();
  });

  it('saves a task\'s file list in order even when the first save answers last', async () => {
    let finishFirst!: (task: unknown) => void;
    vi.mocked(TaskService.updateTask).mockImplementationOnce(() => new Promise(resolve => { finishFirst = resolve; }) as never);
    renderChat();

    await upload.onTasksUpdated!('uploads/g/s/first.pdf');
    await upload.onTasksUpdated!('uploads/g/s/second.pdf');
    await new Promise(resolve => setTimeout(resolve, 0));
    // The second list waits for the first save, so it cannot be overwritten by it.
    expect(TaskService.updateTask).toHaveBeenCalledTimes(1);

    finishFirst({});
    await waitFor(() => expect(TaskService.updateTask).toHaveBeenCalledTimes(2));
    expect(lastTaskWrite()?.tool_configs?.[KNOWLEDGE]).toEqual(
      expect.objectContaining({ file_paths: ['uploads/g/s/first.pdf', 'uploads/g/s/second.pdf'] }));
  });
});
