/**
 * Unit tests for taskExecutionStore - guarded state transitions and lifecycle management.
 *
 * Tests the FSM-based transition system that prevents invalid state changes,
 * enforces lifecycle precedence, and provides batch transition capabilities.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { useTaskExecutionStore } from './taskExecutionStore';

// Reset store state before each test
beforeEach(() => {
  useTaskExecutionStore.getState().clearTaskStates();
  useTaskExecutionStore.getState().setIsPlanningPhase(false);
});

describe('taskExecutionStore - transition()', () => {
  it('should allow initial transition for a new task', () => {
    const result = useTaskExecutionStore.getState().transition('task-1', 'running', {
      task_name: 'Test Task',
    });

    expect(result).toBe(true);
    const state = useTaskExecutionStore.getState().getTaskStatus('task-1');
    expect(state?.status).toBe('running');
    expect(state?.task_name).toBe('Test Task');
    expect(state?.started_at).toBeDefined();
  });

  it('should allow planning -> running transition', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'planning', { task_name: 'Test' });
    const result = store.transition('task-1', 'running');

    expect(result).toBe(true);
    expect(store.getTaskStatus('task-1')?.status).toBe('running');
  });

  it('should allow running -> completed transition', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });
    const result = store.transition('task-1', 'completed');

    expect(result).toBe(true);
    expect(store.getTaskStatus('task-1')?.status).toBe('completed');
    expect(store.getTaskStatus('task-1')?.completed_at).toBeDefined();
  });

  it('should allow running -> failed transition', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });
    const result = store.transition('task-1', 'failed');

    expect(result).toBe(true);
    expect(store.getTaskStatus('task-1')?.status).toBe('failed');
    expect(store.getTaskStatus('task-1')?.failed_at).toBeDefined();
  });

  it('should allow failed -> running transition (retry)', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });
    store.transition('task-1', 'failed');
    const result = store.transition('task-1', 'running');

    expect(result).toBe(true);
    expect(store.getTaskStatus('task-1')?.status).toBe('running');
  });

  it('should block completed -> running transition (backward)', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });
    store.transition('task-1', 'completed');
    const result = store.transition('task-1', 'running');

    expect(result).toBe(false);
    expect(store.getTaskStatus('task-1')?.status).toBe('completed');
  });

  it('should block completed -> planning transition (backward)', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'completed', { task_name: 'Test' });
    const result = store.transition('task-1', 'planning');

    expect(result).toBe(false);
    expect(store.getTaskStatus('task-1')?.status).toBe('completed');
  });

  it('should return true for same-status no-op', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });
    const result = store.transition('task-1', 'running');

    expect(result).toBe(true);
    expect(store.getTaskStatus('task-1')?.status).toBe('running');
  });

  it('should block planning -> completed (invalid, must go through running)', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'planning', { task_name: 'Test' });
    const result = store.transition('task-1', 'completed');

    expect(result).toBe(false);
    expect(store.getTaskStatus('task-1')?.status).toBe('planning');
  });

  it('should preserve started_at timestamp through transitions', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', {
      task_name: 'Test',
      started_at: '2024-01-01T00:00:00Z',
    });
    store.transition('task-1', 'completed');

    const state = store.getTaskStatus('task-1');
    expect(state?.started_at).toBe('2024-01-01T00:00:00Z');
    expect(state?.completed_at).toBeDefined();
  });

  it('should set default task_name for new task when not provided', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running');

    expect(store.getTaskStatus('task-1')?.task_name).toBe('');
  });

  it('should set completed_at for new task created in completed status', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'completed', { task_name: 'Done Task' });

    const state = store.getTaskStatus('task-1');
    expect(state?.status).toBe('completed');
    expect(state?.completed_at).toBeDefined();
  });
});

describe('taskExecutionStore - transitionAll()', () => {
  it('should transition all matching tasks', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Task 1' });
    store.transition('task-2', 'running', { task_name: 'Task 2' });
    store.transition('task-3', 'planning', { task_name: 'Task 3' });

    store.transitionAll(['running', 'planning'], 'completed');

    expect(store.getTaskStatus('task-1')?.status).toBe('completed');
    expect(store.getTaskStatus('task-2')?.status).toBe('completed');
    expect(store.getTaskStatus('task-3')?.status).toBe('completed');
  });

  it('should not transition tasks not in fromStatuses', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Task 1' });
    store.transition('task-2', 'completed', { task_name: 'Task 2' });

    store.transitionAll(['running'], 'failed');

    expect(store.getTaskStatus('task-1')?.status).toBe('failed');
    expect(store.getTaskStatus('task-2')?.status).toBe('completed'); // unchanged
  });

  it('should respect precedence in batch transitions', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'completed', { task_name: 'Task 1' });
    store.transition('task-2', 'running', { task_name: 'Task 2' });

    // Try to transition everything to 'running' - should not downgrade completed
    store.transitionAll(['completed', 'running'], 'running');

    expect(store.getTaskStatus('task-1')?.status).toBe('completed'); // not downgraded
    expect(store.getTaskStatus('task-2')?.status).toBe('running'); // no-op (same status)
  });

  it('should set completed_at on batch completion', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Task 1' });
    store.transition('task-2', 'running', { task_name: 'Task 2' });

    store.transitionAll(['running'], 'completed', {
      completed_at: '2024-06-01T12:00:00Z',
    });

    expect(store.getTaskStatus('task-1')?.completed_at).toBe('2024-06-01T12:00:00Z');
    expect(store.getTaskStatus('task-2')?.completed_at).toBe('2024-06-01T12:00:00Z');
  });

  it('should not update state when no tasks match', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'completed', { task_name: 'Task 1' });

    // Try to transition 'running' tasks - none exist
    store.transitionAll(['running'], 'failed');

    expect(store.getTaskStatus('task-1')?.status).toBe('completed');
  });
});

describe('taskExecutionStore - clearTaskStates', () => {
  it('should clear task states but preserve isPlanningPhase', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });
    store.setIsPlanningPhase(true);

    store.clearTaskStates();

    expect(store.taskStates.size).toBe(0);
    expect(useTaskExecutionStore.getState().isPlanningPhase).toBe(true);
  });

  it('should allow new transitions after clearing', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'completed', { task_name: 'Test' });

    store.clearTaskStates();

    // Now the same task can be set to running (no stale completed state blocking it)
    const result = store.transition('task-1', 'running', { task_name: 'Test' });
    expect(result).toBe(true);
    expect(store.getTaskStatus('task-1')?.status).toBe('running');
  });
});

describe('taskExecutionStore - isPlanningPhase', () => {
  it('should default to false', () => {
    expect(useTaskExecutionStore.getState().isPlanningPhase).toBe(false);
  });

  it('should be settable to true', () => {
    useTaskExecutionStore.getState().setIsPlanningPhase(true);
    expect(useTaskExecutionStore.getState().isPlanningPhase).toBe(true);
  });

  it('should be settable back to false', () => {
    const store = useTaskExecutionStore.getState();
    store.setIsPlanningPhase(true);
    store.setIsPlanningPhase(false);
    expect(useTaskExecutionStore.getState().isPlanningPhase).toBe(false);
  });
});

describe('taskExecutionStore - isTaskRunning', () => {
  it('should return true for running tasks', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });

    expect(store.isTaskRunning('task-1')).toBe(true);
  });

  it('should return false for completed tasks', () => {
    const store = useTaskExecutionStore.getState();
    store.transition('task-1', 'running', { task_name: 'Test' });
    store.transition('task-1', 'completed');

    expect(store.isTaskRunning('task-1')).toBe(false);
  });

  it('should return false for unknown tasks', () => {
    expect(useTaskExecutionStore.getState().isTaskRunning('unknown')).toBe(false);
  });
});
