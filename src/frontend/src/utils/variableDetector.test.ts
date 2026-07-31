import { describe, it, expect } from 'vitest';
import {
  detectVariablesFromNodes,
  detectVariablesFromGenerated,
  DetectedVariable,
} from './variableDetector';

describe('detectVariablesFromNodes', () => {
  it('detects variables from an agentNode data fields', () => {
    const result = detectVariablesFromNodes([
      {
        type: 'agentNode',
        data: {
          role: 'A {role_var} expert',
          goal: 'Achieve {goal_var}',
          backstory: 'Born in {place}',
        },
      },
    ]);
    expect(result).toEqual(
      expect.arrayContaining<DetectedVariable>([
        { name: 'role_var', required: true },
        { name: 'goal_var', required: true },
        { name: 'place', required: true },
      ]),
    );
    expect(result).toHaveLength(3);
  });

  it('detects variables from an agent type node', () => {
    const result = detectVariablesFromNodes([
      { type: 'agent', data: { role: '{r}' } },
    ]);
    expect(result).toEqual([{ name: 'r', required: true }]);
  });

  it('detects variables from a taskNode covering description and expected_output', () => {
    const result = detectVariablesFromNodes([
      {
        type: 'taskNode',
        data: {
          description: 'Do {task_desc}',
          expected_output: 'Produce {output_var}',
          label: 'Label {label_var}',
        },
      },
    ]);
    expect(result).toEqual(
      expect.arrayContaining<DetectedVariable>([
        { name: 'task_desc', required: true },
        { name: 'output_var', required: true },
        { name: 'label_var', required: true },
      ]),
    );
    expect(result).toHaveLength(3);
  });

  it('detects variables from a task type node', () => {
    const result = detectVariablesFromNodes([
      { type: 'task', data: { goal: '{g}' } },
    ]);
    expect(result).toEqual([{ name: 'g', required: true }]);
  });

  it('skips non-object nodes (null, undefined, primitives)', () => {
    const result = detectVariablesFromNodes([
      null,
      undefined,
      42,
      'a string {ignored}',
      false,
    ]);
    expect(result).toEqual([]);
  });

  it('skips nodes whose type is not agent/task', () => {
    const result = detectVariablesFromNodes([
      { type: 'otherNode', data: { role: '{ignored}' } },
      { type: undefined, data: { role: '{ignored2}' } },
    ]);
    expect(result).toEqual([]);
  });

  it('handles a node without data', () => {
    const result = detectVariablesFromNodes([{ type: 'agentNode' }]);
    expect(result).toEqual([]);
  });

  it('handles a node whose data is not an object', () => {
    const result = detectVariablesFromNodes([
      { type: 'agentNode', data: 'not-an-object' as unknown as Record<string, unknown> },
    ]);
    expect(result).toEqual([]);
  });

  it('handles a node whose data is null', () => {
    const result = detectVariablesFromNodes([
      { type: 'agentNode', data: null as unknown as Record<string, unknown> },
    ]);
    expect(result).toEqual([]);
  });

  it('ignores non-string field values', () => {
    const result = detectVariablesFromNodes([
      {
        type: 'agentNode',
        data: { role: 12345, goal: { nested: '{ignored}' }, backstory: '{kept}' },
      },
    ]);
    expect(result).toEqual([{ name: 'kept', required: true }]);
  });

  it('dedupes the same variable appearing across multiple nodes and fields', () => {
    const result = detectVariablesFromNodes([
      { type: 'agentNode', data: { role: 'use {shared}', goal: 'still {shared}' } },
      { type: 'taskNode', data: { description: 'again {shared}' } },
    ]);
    expect(result).toEqual([{ name: 'shared', required: true }]);
  });

  it('returns empty array for empty input', () => {
    expect(detectVariablesFromNodes([])).toEqual([]);
  });
});

describe('detectVariablesFromGenerated', () => {
  it('detects variables from both agents and tasks arrays', () => {
    const result = detectVariablesFromGenerated(
      [{ role: 'a {agent_var}' }],
      [{ description: 'b {task_var}' }],
    );
    expect(result).toEqual(
      expect.arrayContaining<DetectedVariable>([
        { name: 'agent_var', required: true },
        { name: 'task_var', required: true },
      ]),
    );
    expect(result).toHaveLength(2);
  });

  it('dedupes variables across agents and tasks', () => {
    const result = detectVariablesFromGenerated(
      [{ role: '{dup}' }],
      [{ description: '{dup}' }],
    );
    expect(result).toEqual([{ name: 'dup', required: true }]);
  });

  it('returns empty array when nothing matches', () => {
    expect(
      detectVariablesFromGenerated(
        [{ role: 'no vars here' }],
        [{ description: 'none' }],
      ),
    ).toEqual([]);
  });

  it('handles empty arrays', () => {
    expect(detectVariablesFromGenerated([], [])).toEqual([]);
  });

  it('ignores non-string fields in generated objects', () => {
    const result = detectVariablesFromGenerated(
      [{ role: 99 as unknown as string, goal: '{ok}' }],
      [{}],
    );
    expect(result).toEqual([{ name: 'ok', required: true }]);
  });
});
