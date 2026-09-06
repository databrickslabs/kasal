import { describe, expect, it } from 'vitest';
import {
  buildTranscript,
  draftFailedStep,
  draftMessage,
  draftedStep,
  draftingStep,
  parseSkillCommand,
  toSkillMarkdown,
} from './skillCommand';
import type { ChatMessage } from '../types/chat';

describe('parseSkillCommand', () => {
  it('bare /skill captures the conversation', () => {
    expect(parseSkillCommand('/skill')).toEqual({ mode: 'capture', request: '' });
    expect(parseSkillCommand('/skill capture')).toEqual({ mode: 'capture', request: '' });
  });

  it('/skill <request> drafts from the request', () => {
    expect(parseSkillCommand('/skill writing QBR summaries')).toEqual({
      mode: 'blank',
      request: 'writing QBR summaries',
    });
  });

  it('plain language creation asks are recognised, questions are not', () => {
    expect(parseSkillCommand('create a skill for writing release notes')?.mode).toBe('blank');
    expect(parseSkillCommand('Save what we learned in this chat as a skill')?.mode).toBe('capture');
    expect(parseSkillCommand('turn this conversation into a skill')?.mode).toBe('capture');
    expect(parseSkillCommand('which skills do I have?')).toBeNull();
    expect(parseSkillCommand('that was a skill issue')).toBeNull();
    expect(parseSkillCommand('compare snowflake with databricks')).toBeNull();
  });
});

describe('buildTranscript', () => {
  it('keeps user/assistant turns, drops system and empty, tails to the limit', () => {
    const msgs = [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: '' },
      { role: 'assistant', content: 'hello' },
    ] as ChatMessage[];
    expect(buildTranscript(msgs)).toEqual([
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: 'hello' },
    ]);
    const many = Array.from({ length: 40 }, (_, i) => ({ role: 'user', content: `m${i}` })) as ChatMessage[];
    expect(buildTranscript(many)).toHaveLength(30);
    expect(buildTranscript(many)[0].content).toBe('m10');
  });
});

describe('draft → message', () => {
  const draft = {
    name: 'writing-release-notes',
    description: 'Use when: drafting notes. Trigger when the user mentions releases.',
    body: '# Writing release notes\n\n## When to use this skill\nAny release.',
    valid: true,
    errors: [],
  };

  it('renders SKILL.md with a quoted description the card parser reads back', () => {
    const md = toSkillMarkdown(draft);
    expect(md.startsWith('---\nname: writing-release-notes\ndescription: "Use when: drafting notes.')).toBe(true);
    expect(md.endsWith('Any release.\n')).toBe(true);
  });

  it('wraps the draft in a ```skill block with a one-line intro', () => {
    const msg = draftMessage(draft);
    expect(msg).toMatch(/^Here's a draft of \*\*writing-release-notes\*\*/);
    expect(msg).toContain('\n```skill\n---\nname: writing-release-notes');
    expect(msg.trim().endsWith('```')).toBe(true);
  });

  it('says so when the draft failed validation', () => {
    const msg = draftMessage({ ...draft, valid: false, errors: ['name must be kebab-case'] });
    expect(msg).toContain('did not pass validation');
    expect(msg).toContain('name must be kebab-case');
  });
});

describe('drafting as run activity', () => {
  const draft = {
    name: 'writing-release-notes',
    description: 'd',
    body: 'b',
    valid: true,
    errors: [],
    warnings: [],
    model: 'served-model',
    attempts: 1,
  };

  it('the pending step spins (tool_call, no duration) and names what is being drafted', () => {
    const blank = draftingStep({ mode: 'blank', request: 'a skill for release notes' }, 0, 'm1');
    expect(blank.kind).toBe('tool_call');
    expect(blank.durationMs).toBeUndefined();
    expect(blank.sublabel).toBe('a skill for release notes · m1');
    const capture = draftingStep({ mode: 'capture', request: '' }, 12);
    expect(capture.sublabel).toBe('from this conversation · 12 turns');
  });

  it('the done step reports the served model, the LLM call count and the duration', () => {
    const done = draftedStep(draft, Date.now() - 1500);
    expect(done.kind).toBe('tool_result');
    expect(done.label).toBe('Skill drafted');
    expect(done.sublabel).toBe('writing-release-notes · served-model · 1 LLM call');
    expect(done.durationMs).toBeGreaterThanOrEqual(1500);
    const retried = draftedStep({ ...draft, valid: false, errors: ['bad name'], attempts: 2 }, Date.now());
    expect(retried.label).toBe('Draft needs fixes');
    expect(retried.sublabel).toContain('2 LLM calls');
    expect(retried.detail).toBe('bad name');
  });

  it('a failed call becomes a warning event carrying the reason', () => {
    const failed = draftFailedStep('HTTP 503', Date.now());
    expect(failed.kind).toBe('event');
    expect(failed.label).toBe('⚠ Could not draft the skill: HTTP 503');
    expect(failed.detail).toBe('HTTP 503');
  });
});
