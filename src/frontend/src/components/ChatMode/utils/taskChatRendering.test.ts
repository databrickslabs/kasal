/**
 * How a crew's work reads in chat.
 *
 * Observed: one run streamed a wall of text and THEN showed
 *
 *   **As CNN News & Presentation Lead, orchestrate a crew plan to collect the latest C…**
 *   — description='As CNN News & Presentation Lead, orchestrate a crew plan to collect
 *     the latest CNN news from cnn.com … deployment.\n\nUS…'
 *
 * Three defects in one line: the identity arrived after the output it belonged
 * to, the label was 80 characters of prompt rather than a name, and the body was
 * the same prompt echoed back as a Python repr with visible escapes.
 */
import { describe, it, expect } from 'vitest';
import {
  cleanTaskLabel,
  isEchoedTaskSpec,
  summarizeTaskOutput,
  taskHeaderLabel,
  unescapeLiterals,
} from './taskChatRendering';

const TASK_DESCRIPTION =
  'As CNN News & Presentation Lead, orchestrate a crew plan to collect the latest ' +
  'CNN news from cnn.com, assign agents and tasks, synthesize concise summaries.';

describe('taskHeaderLabel', () => {
  it('prefers the agent role — the task "name" is the whole prompt', () => {
    expect(taskHeaderLabel('Presentation Lead', TASK_DESCRIPTION)).toBe('Presentation Lead');
  });

  it('falls back to the collapsed task line when no role is attributed', () => {
    const label = taskHeaderLabel('', TASK_DESCRIPTION);
    expect(label.length).toBeLessThanOrEqual(81);
    expect(label.startsWith('As CNN News & Presentation Lead')).toBe(true);
  });

  it('ignores machine-ish sources that read worse than the task line', () => {
    expect(taskHeaderLabel('crew', TASK_DESCRIPTION)).not.toBe('crew');
    expect(taskHeaderLabel('system', TASK_DESCRIPTION)).not.toBe('system');
  });

  it('returns empty when there is nothing worth announcing', () => {
    expect(taskHeaderLabel('', '')).toBe('');
  });
});

describe('isEchoedTaskSpec', () => {
  it('catches a repr of the task instead of an answer', () => {
    expect(isEchoedTaskSpec(`description='${TASK_DESCRIPTION}'`, TASK_DESCRIPTION)).toBe(true);
    expect(isEchoedTaskSpec(`expected_output="a deck"`, TASK_DESCRIPTION)).toBe(true);
  });

  it('catches the description restated as the answer', () => {
    expect(isEchoedTaskSpec(TASK_DESCRIPTION, TASK_DESCRIPTION)).toBe(true);
  });

  it('leaves a real answer alone', () => {
    const answer = 'Here are the five top CNN stories this morning, with sources.';
    expect(isEchoedTaskSpec(answer, TASK_DESCRIPTION)).toBe(false);
  });

  it('does not match on a short or missing task name', () => {
    expect(isEchoedTaskSpec('Some output', 'Task')).toBe(false);
    expect(isEchoedTaskSpec('Some output', '')).toBe(false);
  });
});

describe('unescapeLiterals', () => {
  it('turns visible \\n into real breaks', () => {
    expect(unescapeLiterals('deployment.\\n\\nUS news')).toBe('deployment.\n\nUS news');
  });

  it('leaves ordinary text untouched', () => {
    const text = 'A normal answer with no escapes.';
    expect(unescapeLiterals(text)).toBe(text);
  });
});

describe('summarizeTaskOutput', () => {
  it('drops an echoed task spec entirely', () => {
    expect(summarizeTaskOutput(`description='${TASK_DESCRIPTION}'`, null, TASK_DESCRIPTION)).toBeNull();
  });

  it('unescapes what it does keep', () => {
    const out = summarizeTaskOutput('First line.\\n\\nSecond line.', null, TASK_DESCRIPTION);
    expect(out).toBe('First line.\n\nSecond line.');
  });

  it('still drops status pings', () => {
    expect(summarizeTaskOutput('Calling tools.', null, TASK_DESCRIPTION)).toBeNull();
  });

  it('still collapses a very long answer', () => {
    const long = 'x'.repeat(900);
    expect(summarizeTaskOutput(long, null, TASK_DESCRIPTION)?.endsWith('…')).toBe(true);
  });

  it('keeps a genuine answer', () => {
    const answer = 'Top five stories: A, B, C, D, E — each with a source link.';
    expect(summarizeTaskOutput(answer, null, TASK_DESCRIPTION)).toBe(answer);
  });

  it('works without a task name (the polling path may not carry one)', () => {
    const answer = 'A real answer.';
    expect(summarizeTaskOutput(answer, null)).toBe(answer);
  });
});

describe('cleanTaskLabel', () => {
  it('is unchanged — still the fallback for un-headed tasks', () => {
    expect(cleanTaskLabel('Improve the artifact below based on this instruction: x')).toBe(
      'Refined artifact',
    );
    expect(cleanTaskLabel('')).toBe('Task');
  });
});
