import { describe, it, expect } from 'vitest';
import {
  extractTaskId,
  extractTaskName,
  mapEventToStatus,
  findTaskStoreKey,
  type TaskStateEntry,
} from './taskIdUtils';

describe('extractTaskId', () => {
  it('should prefer frontend_task_id from trace_metadata', () => {
    const trace = {
      event_type: 'task_started',
      event_context: 'starting_task',
      trace_metadata: {
        frontend_task_id: 'node-abc-123',
        task_id: 'crewai-uuid',
        task_name: 'Research competitors',
      },
    };
    expect(extractTaskId(trace)).toBe('node-abc-123');
  });

  it('should fall back to task_id when no frontend_task_id', () => {
    const trace = {
      event_type: 'task_started',
      trace_metadata: {
        task_id: 'crewai-uuid',
        task_name: 'Research competitors',
      },
    };
    expect(extractTaskId(trace)).toBe('crewai-uuid');
  });

  it('should fall back to task name when no IDs', () => {
    const trace = {
      event_type: 'task_started',
      event_context: 'Research competitors',
      trace_metadata: {
        task_name: 'Research competitors',
      },
    };
    expect(extractTaskId(trace)).toBe('Research competitors');
  });

  it('should return null when nothing usable', () => {
    const trace = {
      event_type: 'task_started',
      event_context: '',
    };
    expect(extractTaskId(trace)).toBeNull();
  });

  it('should extract name from context when context is a real description', () => {
    const trace = {
      event_type: 'task_started',
      event_context: 'Analyze market trends for Q4',
    };
    expect(extractTaskId(trace)).toBe('Analyze market trends for Q4');
  });
});

describe('extractTaskName', () => {
  it('should use event_context when it is a real task description', () => {
    const trace = {
      event_context: 'Research competitors in the market',
      trace_metadata: { task_name: 'Research competitors in the market' },
    };
    expect(extractTaskName(trace)).toBe('Research competitors in the market');
  });

  it('should fall back to metadata.task_name for generic contexts', () => {
    const trace = {
      event_context: 'starting_task',
      trace_metadata: { task_name: 'Research competitors' },
    };
    expect(extractTaskName(trace)).toBe('Research competitors');
  });

  it('should fall back to metadata.task_name for completing_task', () => {
    const trace = {
      event_context: 'completing_task',
      trace_metadata: { task_name: 'Research competitors' },
    };
    expect(extractTaskName(trace)).toBe('Research competitors');
  });

  it('should fall back to metadata.task_name for task_error', () => {
    const trace = {
      event_context: 'task_error',
      trace_metadata: { task_name: 'Research competitors' },
    };
    expect(extractTaskName(trace)).toBe('Research competitors');
  });

  it('should return null when no usable name', () => {
    const trace = {
      event_context: 'starting_task',
    };
    expect(extractTaskName(trace)).toBeNull();
  });

  it('should skip short context strings', () => {
    const trace = {
      event_context: 'abc',
      trace_metadata: { task_name: 'Proper task name' },
    };
    expect(extractTaskName(trace)).toBe('Proper task name');
  });
});

describe('mapEventToStatus', () => {
  it('should map task_failed to failed', () => {
    expect(mapEventToStatus('task_failed')).toBe('failed');
  });

  it('should map task_completed to completed', () => {
    expect(mapEventToStatus('task_completed')).toBe('completed');
  });

  it('should map task_started to running', () => {
    expect(mapEventToStatus('task_started')).toBe('running');
  });

  it('should default to running for unknown event types', () => {
    expect(mapEventToStatus('agent_execution')).toBe('running');
  });

  it('should default to running when event type is undefined', () => {
    expect(mapEventToStatus(undefined)).toBe('running');
  });
});

describe('findTaskStoreKey', () => {
  const makeStates = (keys: string[]): Map<string, TaskStateEntry> => {
    const map = new Map<string, TaskStateEntry>();
    keys.forEach((key) => {
      map.set(key, { status: 'running', task_name: key });
    });
    return map;
  };

  it('should find by exact nodeTaskId', () => {
    const states = makeStates(['abc-123']);
    expect(findTaskStoreKey(states, 'abc-123', 'Some Label')).toBe('abc-123');
  });

  it('should find by stripped task- prefix', () => {
    const states = makeStates(['abc-123']);
    expect(findTaskStoreKey(states, 'task-abc-123', 'Some Label')).toBe('abc-123');
  });

  it('should find by nodeLabel when taskId does not match', () => {
    const states = makeStates(['Research competitors']);
    expect(findTaskStoreKey(states, 'non-existent-id', 'Research competitors')).toBe(
      'Research competitors'
    );
  });

  it('should return null when nothing matches', () => {
    const states = makeStates(['other-key']);
    expect(findTaskStoreKey(states, 'no-match', 'No Match')).toBeNull();
  });

  it('should prefer taskId over label', () => {
    const states = makeStates(['id-123', 'My Label']);
    expect(findTaskStoreKey(states, 'id-123', 'My Label')).toBe('id-123');
  });

  it('should handle undefined taskId and label', () => {
    const states = makeStates(['some-key']);
    expect(findTaskStoreKey(states, undefined, undefined)).toBeNull();
  });

  it('should handle empty map', () => {
    const states = new Map<string, TaskStateEntry>();
    expect(findTaskStoreKey(states, 'id', 'label')).toBeNull();
  });

  it('should match label case-insensitively', () => {
    const states = makeStates(['Research Competitors']);
    expect(findTaskStoreKey(states, 'no-match', 'research competitors')).toBe(
      'Research Competitors'
    );
  });

  it('should match by stored task_name against label', () => {
    // Key is an ID but stored task_name matches the label
    const map = new Map<string, TaskStateEntry>();
    map.set('crewai-uuid-123', { status: 'running', task_name: 'Research competitors' });
    expect(findTaskStoreKey(map, 'no-match', 'Research competitors')).toBe('crewai-uuid-123');
  });

  it('should match by stored task_name case-insensitively', () => {
    const map = new Map<string, TaskStateEntry>();
    map.set('crewai-uuid-123', { status: 'running', task_name: 'Research Competitors' });
    expect(findTaskStoreKey(map, 'no-match', 'research competitors')).toBe('crewai-uuid-123');
  });

  it('should match when key is a longer description starting with label', () => {
    const states = makeStates(['Research competitors in the AI market for Q4 analysis']);
    expect(findTaskStoreKey(states, 'no-match', 'Research competitors in the AI market')).toBe(
      'Research competitors in the AI market for Q4 analysis'
    );
  });

  it('should match when label is longer and starts with key', () => {
    const states = makeStates(['Research competitors']);
    expect(findTaskStoreKey(states, 'no-match', 'Research competitors in the market')).toBe(
      'Research competitors'
    );
  });

  it('should not prefix-match very short labels (< 5 chars)', () => {
    const states = makeStates(['Research competitors']);
    expect(findTaskStoreKey(states, 'no-match', 'Res')).toBeNull();
  });
});
