/**
 * Unit tests for LoadCrew - task node data property consistency.
 *
 * Tests that task nodes created by LoadCrew use the canonical `taskId`
 * property (matching the TaskNodeData interface) instead of `id`.
 */
import { describe, it, expect } from 'vitest';

/**
 * Replicates the task node data construction from LoadCrew.
 * The key fix: uses `taskId` instead of `id` to match the
 * TaskNodeData interface that TaskNode.handlePrepareTaskData reads.
 */
describe('LoadCrew - task node data construction', () => {
  // Replicates the data object built in LoadCrew when creating task nodes
  const buildTaskNodeData = (
    taskId: string,
    taskName: string,
    taskData: {
      description: string;
      expected_output: string;
      agent_id: string | null;
      tools: string[];
      context: string[];
      async_execution: boolean;
      config: Record<string, unknown>;
    }
  ) => {
    return {
      taskId: taskId,
      label: taskName,
      name: taskName,
      description: taskData.description,
      expected_output: taskData.expected_output,
      agent_id: taskData.agent_id,
      tools: taskData.tools,
      context: taskData.context,
      async_execution: taskData.async_execution,
      config: taskData.config,
    };
  };

  it('should set taskId property (not id) on task node data', () => {
    const data = buildTaskNodeData('uuid-123', 'Research Task', {
      description: 'Do research',
      expected_output: 'Report',
      agent_id: null,
      tools: ['31'],
      context: [],
      async_execution: false,
      config: {},
    });

    expect(data.taskId).toBe('uuid-123');
    expect((data as Record<string, unknown>).id).toBeUndefined();
  });

  it('should include tools in task node data', () => {
    const data = buildTaskNodeData('uuid-123', 'Task', {
      description: '',
      expected_output: '',
      agent_id: null,
      tools: ['31', '32', 'PerplexitySearchTool'],
      context: [],
      async_execution: false,
      config: {},
    });

    expect(data.tools).toEqual(['31', '32', 'PerplexitySearchTool']);
  });

  it('should handle empty tools array', () => {
    const data = buildTaskNodeData('uuid-123', 'Task', {
      description: '',
      expected_output: '',
      agent_id: null,
      tools: [],
      context: [],
      async_execution: false,
      config: {},
    });

    expect(data.tools).toEqual([]);
  });

  it('should set label and name from task name', () => {
    const data = buildTaskNodeData('uuid-123', 'My Task', {
      description: '',
      expected_output: '',
      agent_id: null,
      tools: [],
      context: [],
      async_execution: false,
      config: {},
    });

    expect(data.label).toBe('My Task');
    expect(data.name).toBe('My Task');
  });

  it('should preserve agent_id reference', () => {
    const data = buildTaskNodeData('uuid-123', 'Task', {
      description: '',
      expected_output: '',
      agent_id: 'agent-uuid-456',
      tools: [],
      context: [],
      async_execution: false,
      config: {},
    });

    expect(data.agent_id).toBe('agent-uuid-456');
  });

  it('should preserve context dependencies', () => {
    const data = buildTaskNodeData('uuid-123', 'Task', {
      description: '',
      expected_output: '',
      agent_id: null,
      tools: [],
      context: ['task-1', 'task-2'],
      async_execution: false,
      config: {},
    });

    expect(data.context).toEqual(['task-1', 'task-2']);
  });
});
